import os
import logging
from pathlib import Path
from typing import Dict, Optional
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET

from django.conf import settings
from django.utils import timezone
from datetime import timedelta

logger = logging.getLogger(__name__)
from django.contrib.auth import login
from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import UserCreationForm
from django.http import JsonResponse, HttpResponse, HttpResponseRedirect, HttpResponseBadRequest
from django.shortcuts import render, redirect
from django.urls import reverse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt

from .forms import PaperUploadForm
from .tasks import generate_video_task, get_task_status, update_job_progress_from_files, test_r2_storage_write_task
from celery.result import AsyncResult
from config.celery import app as celery_app


def _get_user_friendly_error(error_type: str, error_detail: str = "") -> str:
    """Convert error type to user-friendly error message.
    
    Args:
        error_type: Error type from task classification
        error_detail: Detailed error message
        
    Returns:
        User-friendly error message
    """
    error_messages = {
        "paper_not_found": "Paper not found in PubMed Central. Please check the PubMed ID or PMC ID and ensure the paper is open-access.",
        "api_key_error": "API key invalid or expired. Please contact the administrator.",
        "timeout": "Pipeline timeout. The video generation took too long. Please try again or contact support.",
        "rate_limit": "API rate limit exceeded. Please wait a few minutes and try again.",
        "pipeline_error": f"Video generation failed: {error_detail[:200] if error_detail else 'Unknown error'}",
        "task_error": "Task execution error. Please try again or contact support.",
        "unknown_error": f"An error occurred during video generation: {error_detail[:200] if error_detail else 'Unknown error'}",
    }
    
    return error_messages.get(error_type, error_messages["unknown_error"])


def _check_video_exists(pmid: str, user=None) -> tuple[bool, Optional[str]]:
    """
    Check if video exists in cloud storage (R2) or local filesystem.
    
    Args:
        pmid: Paper ID
        user: Optional user object for authenticated checks
        
    Returns:
        Tuple of (exists: bool, video_url: Optional[str])
    """
    from web.models import VideoGenerationJob
    import logging
    logger = logging.getLogger(__name__)
    
    # Check database for cloud storage
    try:
        job = None
        if user and user.is_authenticated:
            job = VideoGenerationJob.objects.filter(paper_id=pmid, user=user).order_by('-created_at').first()
        
        # Fallback: if not found with user filter, check without user filter
        if not job:
            job = VideoGenerationJob.objects.filter(paper_id=pmid).order_by('-created_at').first()
        
        if job:
            # First try: Check if final_video FileField is set and file exists in storage
            if job.final_video:
                try:
                    # Check if file actually exists in storage
                    if job.final_video.storage.exists(job.final_video.name):
                        video_url = reverse("serve_video", args=[pmid])
                        logger.debug(f"Video found via final_video FileField: {job.final_video.name}")
                        return True, video_url
                    else:
                        logger.warning(f"final_video FileField set but file not found in storage: {job.final_video.name}")
                except Exception as e:
                    logger.warning(f"Error checking final_video FileField: {e}")
            
            # Second try: Check if final_video_path is set and file exists in storage
            if job.final_video_path and settings.USE_CLOUD_STORAGE:
                try:
                    from django.core.files.storage import default_storage
                    if default_storage.exists(job.final_video_path):
                        # File exists in R2, but FileField might not be set - create URL anyway
                        video_url = reverse("serve_video", args=[pmid])
                        logger.info(f"Video found via final_video_path in R2: {job.final_video_path}")
                        # Note: FileField can't be easily set from path, but we can serve via final_video_path
                        # The FileField will be set properly on future uploads
                        return True, video_url
                    else:
                        logger.warning(f"final_video_path set but file not found in storage: {job.final_video_path}")
                except Exception as e:
                    logger.warning(f"Error checking final_video_path in R2: {e}")
            
            # Third try: If both fields are empty but video exists in R2, search for it
            if not job.final_video and not job.final_video_path and settings.USE_CLOUD_STORAGE:
                try:
                    from django.core.files.storage import default_storage
                    # Try to find video file by searching for patterns
                    # Pattern: videos/YYYY/MM/DD/PMCID_final_video_*.mp4
                    found_path = None
                    
                    # Try common date patterns
                    from datetime import datetime
                    now = datetime.now()
                    
                    # Check recent dates (today and yesterday)
                    for days_ago in range(7):  # Check last 7 days
                        check_date = now - timedelta(days=days_ago)
                        date_path = f"videos/{check_date.year}/{check_date.month:02d}/{check_date.day:02d}/"
                        
                        # Try to find files matching the pattern
                        try:
                            # List files in this date directory
                            if hasattr(default_storage, 'listdir'):
                                try:
                                    _, files = default_storage.listdir(date_path)
                                    for filename in files:
                                        if (filename.startswith(f"{pmid}_final_video_") or filename.startswith(f"{pmid}_recorded_")) and filename.endswith('.mp4'):
                                            found_path = date_path + filename
                                            logger.info(f"Found video in R2 storage: {found_path}")
                                            break
                                except:
                                    pass
                            
                            if found_path:
                                break
                        except:
                            continue
                    
                    if found_path and default_storage.exists(found_path):
                        # Update database with found path
                        job.final_video_path = found_path
                        job.save(update_fields=['final_video_path', 'updated_at'])
                        logger.info(f"Auto-updated final_video_path: {found_path}")
                        video_url = reverse("serve_video", args=[pmid])
                        return True, video_url
                except Exception as e:
                    logger.warning(f"Error searching for video in R2: {e}")
    
    except Exception as e:
        logger.warning(f"Error checking video in database: {e}")
    
    # Fallback: check local filesystem (for development or migration period)
    if not settings.USE_CLOUD_STORAGE:
        output_dir = Path(settings.MEDIA_ROOT) / pmid
        recorded = output_dir / "recorded.mp4"
        if recorded.exists():
            video_url = f"{settings.MEDIA_URL.rstrip('/')}/{pmid}/recorded.mp4"
            return True, video_url
    
    return False, None


def _validate_access_code(access_code: str | None) -> bool:
    """Validate the provided access code against the configured code.
    
    Args:
        access_code: The access code to validate (can be None)
        
    Returns:
        True if the access code is valid, False otherwise
        
    Raises:
        ValueError: If VIDEO_ACCESS_CODE is not configured (server misconfiguration)
    """
    # Get expected code from Django settings (which loads from environment variable)
    expected_code = settings.VIDEO_ACCESS_CODE
    
    # Require access code to be configured for security
    if not expected_code:
        raise ValueError(
            "VIDEO_ACCESS_CODE is not configured. "
            "Please set VIDEO_ACCESS_CODE environment variable for security."
        )
    
    # Require access code to be provided and not just whitespace
    if not access_code:
        return False
    
    # Convert to string and strip whitespace
    access_code_str = str(access_code).strip()
    expected_code_str = str(expected_code).strip()
    
    if not access_code_str:
        return False
    
    # Compare codes (case-sensitive)
    return access_code_str == expected_code_str


def _validate_paper_id(paper_id: str) -> tuple[bool, str]:
    """
    Validate that a paper ID (PMID or PMCID) exists and is available in PubMed Central.
    
    Args:
        paper_id: PubMed ID (e.g., "12345678") or PMC ID (e.g., "PMC10979640")
        
    Returns:
        Tuple of (is_valid, error_message)
        - is_valid: True if paper exists and is in PMC, False otherwise
        - error_message: Error message if invalid, empty string if valid
    """
    paper_id = paper_id.strip()
    
    if not paper_id:
        return False, "Please provide a PubMed ID or PMC ID."
    
    try:
        # Determine if input is PMID or PMCID
        if paper_id.upper().startswith("PMC"):
            # It's a PMCID - try to fetch it directly
            pmcid = paper_id.upper()
            pmc_number = pmcid.replace("PMC", "")
            url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id={pmc_number}&retmode=xml"
            
            try:
                with urllib.request.urlopen(url, timeout=10) as response:
                    xml_data = response.read()
                    # Check if we got valid XML (not an error)
                    root = ET.fromstring(xml_data)
                    # If we can parse it and it has content, it's valid
                    if root is not None:
                        return True, ""
            except urllib.error.HTTPError as e:
                if e.code == 400 or e.code == 404:
                    return False, f"PMC ID '{paper_id}' not found in PubMed Central. Please check the ID and ensure the paper is open-access."
                return False, f"Error checking PMC ID: {e}"
            except ET.ParseError:
                return False, f"PMC ID '{paper_id}' not found or not available in PubMed Central."
            except Exception as e:
                return False, f"Error validating PMC ID: {str(e)}"
        else:
            # It's a PMID - look up the PMCID
            url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={paper_id}&retmode=xml"
            
            try:
                with urllib.request.urlopen(url, timeout=10) as response:
                    xml_data = response.read()
                    root = ET.fromstring(xml_data)
                    
                    # Look for PMC ID in ArticleIdList
                    pmcid = None
                    for article_id in root.findall(".//ArticleId"):
                        if article_id.get("IdType") == "pmc":
                            pmc_id = article_id.text
                            if not pmc_id.startswith("PMC"):
                                pmc_id = f"PMC{pmc_id}"
                            pmcid = pmc_id
                            break
                    
                    if not pmcid:
                        return False, f"PubMed ID '{paper_id}' is not available in PubMed Central. This tool only works with open-access papers in PMC."
                    
                    # Verify the PMCID is accessible
                    pmc_number = pmcid.replace("PMC", "")
                    pmc_url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id={pmc_number}&retmode=xml"
                    
                    try:
                        with urllib.request.urlopen(pmc_url, timeout=10) as pmc_response:
                            pmc_xml = pmc_response.read()
                            ET.fromstring(pmc_xml)  # Verify it's valid XML
                            return True, ""
                    except urllib.error.HTTPError:
                        return False, f"PubMed ID '{paper_id}' is not available in PubMed Central. This tool only works with open-access papers in PMC."
                    except Exception:
                        return False, f"PubMed ID '{paper_id}' is not available in PubMed Central. This tool only works with open-access papers in PMC."
            except urllib.error.HTTPError as e:
                if e.code == 400 or e.code == 404:
                    return False, f"PubMed ID '{paper_id}' not found. Please check the ID and try again."
                return False, f"Error checking PubMed ID: {e}"
            except ET.ParseError:
                return False, f"PubMed ID '{paper_id}' not found or invalid."
            except Exception as e:
                return False, f"Error validating PubMed ID: {str(e)}"
    except Exception as e:
        return False, f"Error validating paper ID: {str(e)}"


def health(request):
    return JsonResponse({"status": "ok"})


def static_debug(request):
    """Debug endpoint to check static files configuration"""
    from django.conf import settings
    from pathlib import Path
    
    try:
        static_root = Path(settings.STATIC_ROOT)
        css_file = static_root / "web" / "css" / "style.css"
        
        info = {
            "STATIC_URL": settings.STATIC_URL,
            "STATIC_ROOT": str(settings.STATIC_ROOT),
            "STATIC_ROOT_exists": static_root.exists(),
            "css_file_path": str(css_file),
            "css_file_exists": css_file.exists(),
            "STATICFILES_STORAGE": settings.STATICFILES_STORAGE,
            "DEBUG": settings.DEBUG,
            "whitenoise_in_middleware": "whitenoise.middleware.WhiteNoiseMiddleware" in settings.MIDDLEWARE,
        }
        
        # Try to read the file if it exists
        if css_file.exists():
            try:
                info["css_file_size"] = css_file.stat().st_size
                info["css_file_readable"] = True
            except Exception as e:
                info["css_file_readable"] = False
                info["css_file_error"] = str(e)
        
        # List files in staticfiles directory
        if static_root.exists():
            try:
                info["staticfiles_contents"] = [str(p.relative_to(static_root)) for p in static_root.rglob("*") if p.is_file()][:20]
            except Exception as e:
                info["staticfiles_list_error"] = str(e)
        else:
            info["error"] = f"STATIC_ROOT directory does not exist at {static_root}"
            # Try to create it
            try:
                static_root.mkdir(parents=True, exist_ok=True)
                info["created_directory"] = True
            except Exception as e:
                info["create_directory_error"] = str(e)
        
        import json
        response = JsonResponse(info)
        response.content = json.dumps(info, indent=2)
        return response
    except Exception as e:
        return JsonResponse({"error": str(e), "type": type(e).__name__}, status=500)


def test_r2_storage(request):
    """
    Test endpoint to verify R2 cloud storage is accessible from both Celery and Server.
    
    This creates a test file in R2 via Celery worker and then reads it from Server.
    This verifies that both services can access the same cloud storage.
    
    Access: /test-r2-storage/
    
    Query parameters:
    - test_celery=1: Also test Celery write (default: always tests)
    """
    from django.core.files.storage import default_storage
    from django.core.files.base import ContentFile
    from .tasks import test_r2_storage_write_task
    import traceback
    
    result = {
        "server_test": {},
        "celery_test": {},
        "cross_service_check": {},
    }
    
    # Test Server (Django) write access
    try:
        # Debug storage configuration
        storage_config_debug = {
            "USE_CLOUD_STORAGE": getattr(settings, 'USE_CLOUD_STORAGE', False),
            "DEFAULT_FILE_STORAGE": getattr(settings, 'DEFAULT_FILE_STORAGE', 'Not set'),
            "STORAGES": getattr(settings, 'STORAGES', {}),
            "AWS_STORAGE_BUCKET_NAME": getattr(settings, 'AWS_STORAGE_BUCKET_NAME', 'Not set'),
            "AWS_S3_ENDPOINT_URL": getattr(settings, 'AWS_S3_ENDPOINT_URL', 'Not set'),
            "actual_storage_backend": type(default_storage).__name__,
            "actual_storage_module": type(default_storage).__module__,
        }
        
        test_filename = f"test_files/server_test_{timezone.now().strftime('%Y%m%d_%H%M%S')}.txt"
        test_content = (
            f"R2 Storage Test - {timezone.now().isoformat()}\n"
            f"Service: Server (Django)\n"
            f"Storage Backend: {type(default_storage).__name__}\n"
            f"USE_CLOUD_STORAGE: {getattr(settings, 'USE_CLOUD_STORAGE', False)}\n"
        )
        
        # Write to cloud storage
        test_file = default_storage.save(test_filename, ContentFile(test_content.encode('utf-8')))
        
        # Verify we can read it back
        if default_storage.exists(test_file):
            read_back = default_storage.open(test_file).read().decode('utf-8')
            
            result["server_test"] = {
                "success": True,
                "message": "R2 storage write test successful from Server",
                "service": "Server (Django)",
                "test_file_path": test_file,
                "test_file_exists": True,
                "test_file_readable": True,
                "test_content_matches": read_back == test_content,
                "storage_backend": type(default_storage).__name__,
                "storage_module": type(default_storage).__module__,
                "use_cloud_storage": getattr(settings, 'USE_CLOUD_STORAGE', False),
                "storage_config_debug": storage_config_debug,
                "timestamp": timezone.now().isoformat(),
            }
            
            # Get file URL if available
            try:
                result["server_test"]["test_file_url"] = default_storage.url(test_file)
            except Exception:
                result["server_test"]["test_file_url"] = "N/A (URL generation failed)"
        else:
            result["server_test"] = {
                "success": False,
                "error": "File was written but does not exist when checked",
                "service": "Server (Django)",
                "test_file_path": test_file,
            }
            
    except Exception as e:
        import sys
        exc_type, exc_value, exc_traceback = sys.exc_info()
        result["server_test"] = {
            "success": False,
            "error": str(e),
            "type": type(e).__name__,
            "service": "Server (Django)",
            "traceback": ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback)),
            "use_cloud_storage": getattr(settings, 'USE_CLOUD_STORAGE', False),
            "storage_backend": type(default_storage).__name__ if 'default_storage' in locals() else "unknown",
            "recommendation": "Check R2 credentials and configuration. Ensure USE_CLOUD_STORAGE=True and all AWS_* variables are set.",
        }
    
    # Test Celery write access
    try:
        celery_task = test_r2_storage_write_task.delay()
        celery_result = celery_task.get(timeout=30)  # Increased timeout for cloud operations
        result["celery_test"] = celery_result
        
        # CRITICAL: Check if Server can read the file that Celery wrote
        if celery_result.get("success") and celery_result.get("test_file_path"):
            celery_file_path = celery_result["test_file_path"]
            try:
                # Add a small delay to ensure file is fully written (cloud storage eventual consistency)
                import time
                time.sleep(1)
                
                # Try multiple methods to verify the file exists
                file_exists = False
                file_readable = False
                celery_content = None
                file_url = None
                debug_info = {}
                
                # Method 1: Check if file exists
                try:
                    file_exists = default_storage.exists(celery_file_path)
                    debug_info["exists_check"] = file_exists
                except Exception as e:
                    debug_info["exists_check_error"] = str(e)
                    debug_info["exists_check_error_type"] = type(e).__name__
                
                # Method 2: Try to open the file directly
                if file_exists:
                    try:
                        celery_file = default_storage.open(celery_file_path, 'rb')
                        celery_content = celery_file.read().decode('utf-8')
                        celery_file.close()
                        file_readable = True
                        debug_info["open_success"] = True
                    except Exception as e:
                        debug_info["open_error"] = str(e)
                        debug_info["open_error_type"] = type(e).__name__
                
                # Method 3: Try to get the URL
                try:
                    file_url = default_storage.url(celery_file_path)
                    debug_info["url_generated"] = True
                    debug_info["url"] = file_url
                except Exception as e:
                    debug_info["url_error"] = str(e)
                    debug_info["url_error_type"] = type(e).__name__
                
                # Method 4: List files in the test_files directory to see what's actually there
                try:
                    # Try to list files in the test_files directory
                    test_dir = "test_files/"
                    if hasattr(default_storage, 'listdir'):
                        dirs, files = default_storage.listdir(test_dir)
                        debug_info["listdir_files"] = files[:10]  # First 10 files
                        debug_info["celery_file_in_list"] = celery_file_path.split('/')[-1] in files
                    else:
                        debug_info["listdir_not_available"] = "Storage backend doesn't support listdir"
                except Exception as e:
                    debug_info["listdir_error"] = str(e)
                
                # Method 5: Check storage backend details
                debug_info["storage_backend"] = type(default_storage).__name__
                debug_info["storage_module"] = type(default_storage).__module__
                if hasattr(default_storage, 'bucket_name'):
                    debug_info["bucket_name"] = default_storage.bucket_name
                if hasattr(default_storage, 'location'):
                    debug_info["location"] = default_storage.location
                
                if file_exists and file_readable:
                    result["cross_service_check"] = {
                        "celery_file_path": celery_file_path,
                        "server_can_see_celery_file": True,
                        "server_can_read_celery_file": True,
                        "file_content_preview": celery_content[:300] if celery_content else None,
                        "file_url": file_url,
                        "debug_info": debug_info,
                    }
                else:
                    result["cross_service_check"] = {
                        "celery_file_path": celery_file_path,
                        "server_can_see_celery_file": file_exists,
                        "server_can_read_celery_file": file_readable,
                        "warning": "Server cannot see/read file written by Celery! They may be using different storage backends or credentials.",
                        "debug_info": debug_info,
                    }
            except Exception as e:
                import traceback
                result["cross_service_check"] = {
                    "celery_file_path": celery_file_path,
                    "server_can_see_celery_file": False,
                    "server_can_read_celery_file": False,
                    "read_error": str(e),
                    "error_type": type(e).__name__,
                    "traceback": traceback.format_exc(),
                }
        else:
            result["cross_service_check"] = {
                "warning": "Cannot test cross-service access - Celery write test failed",
                "celery_error": celery_result.get("error", "Unknown error"),
            }
            
    except Exception as e:
        result["celery_test"] = {
            "success": False,
            "error": str(e),
            "type": type(e).__name__,
            "service": "Celery Worker",
            "recommendation": "Check that Celery worker is running and has access to R2 credentials.",
        }
        result["cross_service_check"] = {
            "warning": "Cannot test cross-service access - Celery task failed",
            "error": str(e),
        }
    
    # Overall status
    try:
        server_ok = result.get("server_test", {}).get("success", False)
        celery_test_result = result.get("celery_test", {})
        celery_ok = celery_test_result.get("success", False) if celery_test_result else None
        cross_service_ok = result.get("cross_service_check", {}).get("server_can_see_celery_file", None)
        
        if celery_ok is None:
            # Only tested server
            result["overall_status"] = "OK" if server_ok else "FAILED"
            result["recommendation"] = "Server can access R2 storage" if server_ok else "Server cannot access R2 storage. Check R2 credentials and configuration."
        else:
            # Tested both - now check cross-service access
            if server_ok and celery_ok and cross_service_ok:
                result["overall_status"] = "OK"
                result["recommendation"] = "✅ Both Server and Celery can access the SAME R2 storage. Setup is correct!"
            elif server_ok and celery_ok and not cross_service_ok:
                result["overall_status"] = "FAILED"
                result["recommendation"] = "⚠️ CRITICAL: Server and Celery are using DIFFERENT storage backends or credentials! Celery can write, but Server cannot see Celery's files. Check that both services have the same R2 environment variables set."
            elif not server_ok and celery_ok:
                result["overall_status"] = "PARTIAL"
                result["recommendation"] = "Celery can write to R2, but Server cannot. Check Server's R2 credentials and configuration."
            elif server_ok and not celery_ok:
                result["overall_status"] = "PARTIAL"
                result["recommendation"] = "Server can access R2, but Celery cannot write. Check Celery's R2 credentials and configuration."
            else:
                result["overall_status"] = "FAILED"
                result["recommendation"] = "Neither service can access R2 storage. Check R2 credentials and configuration for both services."
        
        status_code = 200 if result.get("overall_status") == "OK" else 500
    except Exception as e:
        result["overall_status"] = "ERROR"
        result["status_calculation_error"] = str(e)
        result["recommendation"] = "Error calculating overall status. Check individual test results."
        status_code = 500
    
    import json
    response = JsonResponse(result, status=status_code)
    response.content = json.dumps(result, indent=2)
    return response


def debug_video_files(request, pmid: str):
    """
    Debug endpoint to list all files in the video output directory.
    
    This helps diagnose issues where the video file might not be detected
    even though the pipeline completed successfully.
    
    Access: /debug-video-files/<pmid>/
    """
    from pathlib import Path
    from django.conf import settings
    from django.http import JsonResponse
    import traceback
    import os
    import subprocess
    
    try:
        media_root = Path(settings.MEDIA_ROOT)
        output_dir = media_root / pmid
        
        # Check if MEDIA_ROOT is actually on a mounted volume
        volume_info = {}
        try:
            # Check filesystem info
            stat_info = os.statvfs(str(media_root))
            volume_info["filesystem"] = {
                "f_fsid": stat_info.f_fsid,
                "f_type": stat_info.f_type,  # Filesystem type
                "total_space_gb": (stat_info.f_blocks * stat_info.f_frsize) / (1024**3),
                "free_space_gb": (stat_info.f_bavail * stat_info.f_frsize) / (1024**3),
            }
            
            # Try to check if it's a mount point using df command
            try:
                df_result = subprocess.run(
                    ["df", "-T", str(media_root)],
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                if df_result.returncode == 0:
                    lines = df_result.stdout.strip().split('\n')
                    if len(lines) > 1:
                        parts = lines[1].split()
                        if len(parts) >= 2:
                            volume_info["mount_info"] = {
                                "filesystem_type": parts[1],
                                "mount_point": parts[-1] if len(parts) > 1 else "unknown",
                            }
            except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
                volume_info["mount_check_error"] = str(e)
        except Exception as e:
            volume_info["error"] = str(e)
        
        result = {
            "pmid": pmid,
            "output_dir": str(output_dir),
            "output_dir_exists": output_dir.exists(),
            "MEDIA_ROOT": str(media_root),
            "MEDIA_ROOT_exists": media_root.exists(),
            "volume_info": volume_info,
            "files": [],
            "directories": [],
            "media_root_contents": [],  # List everything in MEDIA_ROOT
        }
        
        # First, list everything in MEDIA_ROOT to see what's actually on the volume
        if media_root.exists():
            try:
                for item in media_root.iterdir():
                    try:
                        if item.is_file():
                            stat = item.stat()
                            result["media_root_contents"].append({
                                "name": item.name,
                                "type": "file",
                                "size": stat.st_size,
                                "modified": stat.st_mtime,
                            })
                        elif item.is_dir():
                            # Calculate directory size
                            dir_size = 0
                            file_count = 0
                            try:
                                for f in item.rglob('*'):
                                    if f.is_file():
                                        try:
                                            dir_size += f.stat().st_size
                                            file_count += 1
                                        except:
                                            pass
                            except:
                                pass
                            
                            dir_info = {
                                "name": item.name,
                                "type": "directory",
                                "size": dir_size,
                                "file_count": file_count,
                            }
                            
                            # Check if this directory contains recorded.mp4
                            recorded_in_dir = item / "recorded.mp4"
                            if recorded_in_dir.exists():
                                try:
                                    dir_info["has_final_video"] = True
                                    dir_info["final_video_size"] = recorded_in_dir.stat().st_size
                                except:
                                    dir_info["has_final_video"] = True
                                    dir_info["final_video_size"] = "unknown"
                            else:
                                dir_info["has_final_video"] = False
                            
                            result["media_root_contents"].append(dir_info)
                    except Exception as e:
                        result["media_root_contents"].append({
                            "name": item.name if hasattr(item, 'name') else str(item),
                            "type": "unknown",
                            "error": str(e),
                        })
            except Exception as e:
                result["media_root_scan_error"] = str(e)
        
        # Check if we can see files written by Celery (the real volume test)
        celery_test_dir = media_root / ".volume_test"
        celery_test_file = celery_test_dir / "test_write_celery.txt"
        result["celery_file_check"] = {
            "test_dir_exists": celery_test_dir.exists(),
            "test_file_exists": celery_test_file.exists(),
            "test_file_path": str(celery_test_file),
        }
        if celery_test_file.exists():
            try:
                celery_content = celery_test_file.read_text()
                result["celery_file_check"]["readable"] = True
                result["celery_file_check"]["content_preview"] = celery_content[:200]
            except Exception as e:
                result["celery_file_check"]["readable"] = False
                result["celery_file_check"]["read_error"] = str(e)
        
        if output_dir.exists():
            try:
                # List all files recursively
                for file_path in output_dir.rglob("*"):
                    try:
                        if file_path.is_file():
                            try:
                                stat = file_path.stat()
                                result["files"].append({
                                    "path": str(file_path.relative_to(output_dir)),
                                    "full_path": str(file_path),
                                    "exists": file_path.exists(),
                                    "size": stat.st_size,
                                    "modified": stat.st_mtime,
                                })
                            except Exception as e:
                                result["files"].append({
                                    "path": str(file_path) if not str(file_path).startswith(str(output_dir)) else str(file_path.relative_to(output_dir)),
                                    "full_path": str(file_path),
                                    "exists": file_path.exists(),
                                    "error": str(e),
                                })
                        elif file_path.is_dir():
                            try:
                                rel_path = str(file_path.relative_to(output_dir))
                                if rel_path not in result["directories"]:
                                    result["directories"].append(rel_path)
                            except Exception:
                                # If relative_to fails, just use the full path
                                if str(file_path) not in result["directories"]:
                                    result["directories"].append(str(file_path))
                    except Exception as e:
                        # Skip files/dirs we can't access
                        result["files"].append({
                            "path": "unknown",
                            "full_path": str(file_path) if hasattr(file_path, '__str__') else "unknown",
                            "error": str(e),
                        })
            except Exception as e:
                result["scan_error"] = str(e)
                result["scan_traceback"] = traceback.format_exc()
        
        # Check specifically for recorded.mp4 (the playable output video)
        recorded = output_dir / "recorded.mp4"
        try:
            recorded_exists = recorded.exists()
            recorded_size = 0
            if recorded_exists:
                try:
                    recorded_size = recorded.stat().st_size
                except Exception as e:
                    recorded_size = f"error: {str(e)}"
            
            result["final_video_check"] = {
                "expected_path": str(recorded),
                "exists": recorded_exists,
                "size": recorded_size,
            }
        except Exception as e:
            result["final_video_check"] = {
                "expected_path": str(recorded),
                "error": str(e),
            }
        
        import json
        response = JsonResponse(result)
        response.content = json.dumps(result, indent=2)
        return response
    except Exception as e:
        import sys
        error_response = JsonResponse({
            "error": str(e),
            "type": type(e).__name__,
            "traceback": traceback.format_exc(),
            "pmid": pmid,
        }, status=500)
        error_response.content = json.dumps({
            "error": str(e),
            "type": type(e).__name__,
            "traceback": traceback.format_exc(),
            "pmid": pmid,
        }, indent=2)
        return error_response


def home(request):
    # Render the beautiful landing page
    return render(request, "landing.html")


def _get_completed_steps_from_progress(progress_percent: int) -> list:
    """Convert progress percent to list of completed step names."""
    steps = [
        ("fetch-paper", 25),
        ("generate-script", 50),
        ("generate-audio", 75),
        ("generate-videos", 100),
    ]
    
    completed_steps = []
    for step_name, step_percent in steps:
        if progress_percent >= step_percent:
            completed_steps.append(step_name)
    
    return completed_steps


def _get_pipeline_progress(output_dir: Path) -> Dict:
    """Check pipeline progress by examining output directory for step completion markers.
    
    Also checks Celery task status for error information.
    
    Returns a dict with:
    - current_step: name of current step or None if complete
    - completed_steps: list of completed step names
    - progress_percent: 0-100
    - status: 'pending', 'running', 'completed', 'failed'
    - error: error message if failed (from Celery task)
    - error_type: user-friendly error type
    """
    steps = [
        ("fetch-paper", lambda d: (d / "paper.json").exists()),
        ("generate-script", lambda d: (d / "script.json").exists()),
        ("generate-audio", lambda d: (d / "audio.wav").exists() and (d / "audio_metadata.json").exists()),
        ("generate-videos", lambda d: (d / "clips" / ".videos_complete").exists() or (d / "recorded.mp4").exists()),
    ]
    
    completed_steps = []
    current_step = None
    
    for step_name, check_func in steps:
        if check_func(output_dir):
            completed_steps.append(step_name)
        else:
            if current_step is None:
                current_step = step_name
            break
    
    total_steps = len(steps)
    completed_count = len(completed_steps)
    progress_percent = int((completed_count / total_steps) * 100)
    
    # Check if pipeline failed (has log but no final video and not currently running)
    log_path = output_dir / "pipeline.log"
    recorded = output_dir / "recorded.mp4"
    
    error = None
    error_type = None
    status = "pending"  # Initialize status
    
    # Priority 0: Check if final video exists (completed) - this is the most definitive check
    # Check this FIRST before anything else - if video exists, we're done
    # Check both local filesystem and R2 storage
    video_exists = False
    if settings.USE_CLOUD_STORAGE:
        # Check R2 storage via database
        try:
            from web.models import VideoGenerationJob
            pmid = output_dir.name
            job = VideoGenerationJob.objects.filter(paper_id=pmid).order_by('-created_at').first()
            if job and job.final_video:
                try:
                    video_exists = job.final_video.storage.exists(job.final_video.name)
                except Exception:
                    pass
        except Exception:
            pass
    
    # Fallback to local filesystem check
    if not video_exists:
        video_exists = output_dir.exists() and recorded.exists()
    
    if video_exists:
        status = "completed"
        return {
            "current_step": None,
            "completed_steps": completed_steps,
            "progress_percent": 100,
            "status": "completed",
            "total_steps": total_steps,
        }
    
    # Check Celery task status for error information FIRST (most reliable)
    # Method 1: Try to get task status directly from Celery's result backend
    task_result = None
    pmid = output_dir.name
    task_id_file = output_dir / "task_id.txt"
    
    # Try to get task status from Celery result backend first (most reliable)
    if task_id_file.exists():
        try:
            with open(task_id_file, "r") as f:
                task_id = f.read().strip()
            if task_id:
                async_result = AsyncResult(task_id, app=celery_app)
                if async_result.ready():
                    # Task has completed (success or failure)
                    try:
                        result = async_result.get(timeout=1)  # Quick timeout
                        if isinstance(result, dict):
                            task_result = result
                            # Ensure status is set correctly
                            if async_result.failed():
                                task_result["status"] = "failed"
                            elif async_result.successful() and result.get("status") == "failed":
                                # Task succeeded from Celery's perspective but pipeline failed
                                task_result["status"] = "failed"
                    except Exception as e:
                        # If we can't get result, check if task failed
                        if async_result.failed():
                            try:
                                task_result = {
                                    "status": "failed",
                                    "error": str(async_result.info) if async_result.info else "Task failed",
                                    "error_type": "task_error"
                                }
                            except:
                                pass
        except Exception:
            pass  # Fall through to file-based check
    
    # Method 2: Fall back to reading task_result.json file
    if not task_result:
        task_result = get_task_status(pmid)
    
    # Priority 1: Check task result FIRST (most reliable source of truth)
    # This should be checked before anything else to catch failures immediately
    if task_result:
        task_status = task_result.get("status")
        if task_status == "failed":
            status = "failed"
            error = task_result.get("error")
            error_type = task_result.get("error_type")
            # Don't check anything else - task result is definitive
        elif task_status == "completed":
            # Verify final video exists to confirm completion (check both R2 and local)
            video_exists = False
            if settings.USE_CLOUD_STORAGE:
                # Check R2 storage via database
                try:
                    from web.models import VideoGenerationJob
                    job = VideoGenerationJob.objects.filter(paper_id=pmid).order_by('-created_at').first()
                    if job and job.final_video:
                        try:
                            video_exists = job.final_video.storage.exists(job.final_video.name)
                        except Exception:
                            pass
                except Exception:
                    pass
            
            # Fallback to local filesystem check
            if not video_exists:
                video_exists = recorded.exists()
            
            if video_exists:
                status = "completed"
            else:
                # Task says completed but video doesn't exist - might still be processing
                status = "running"
        elif task_status == "running":
            # Task says running, but check log for failure indicators (task might have failed but not updated status yet)
            if log_path.exists():
                try:
                    with open(log_path, "rb") as f:
                        f.seek(max(0, f.tell() - 8192))
                        log_content = f.read().decode(errors="replace")
                        # Check for various failure indicators in log
                        log_lower = log_content.lower()
                        if ("pipeline failed" in log_lower or 
                            ("✗" in log_content and "failed" in log_lower) or
                            "http error" in log_lower or
                            "bad request" in log_lower or
                            "step 'fetch-paper' failed" in log_lower):
                            # Log shows failure even though task says running - trust the log
                            status = "failed"
                            # Extract error from log
                            lines = log_content.split("\n")
                            for line in reversed(lines):
                                if (("✗" in line or "failed" in line.lower() or "error" in line.lower()) and 
                                    line.strip() and 
                                    not line.strip().startswith("2025-")):  # Skip timestamp-only lines
                                    if not error:
                                        error = line.strip()
                                    break
                            if not error_type and error:
                                error_lower = error.lower()
                                if "not available in pubmed central" in error_lower:
                                    error_type = "paper_not_found"
                                elif "http error 400" in error_lower or "bad request" in error_lower:
                                    error_type = "pipeline_error"
                                elif "http error 400" in error_lower or "bad request" in error_lower:
                                    error_type = "pipeline_error"
                except:
                    pass
            if status != "failed":
                status = "running"
        else:
            # Task result exists but status is unclear, check other indicators
            if current_step:
                status = "running"
            elif log_path.exists():
                # Check log for failure indicators first
                try:
                    with open(log_path, "rb") as f:
                        f.seek(max(0, f.tell() - 8192))
                        log_content = f.read().decode(errors="replace")
                        if "pipeline failed" in log_content.lower() or ("✗" in log_content and "failed" in log_content.lower()):
                            status = "failed"
                            # Extract error
                            lines = log_content.split("\n")
                            for line in reversed(lines):
                                if ("✗" in line or "failed" in line.lower()) and line.strip():
                                    if not error:
                                        error = line.strip()
                                    break
                except:
                    pass
                
                # If still not failed, check timestamp
                if status != "failed":
                    try:
                        import time
                        mtime = log_path.stat().st_mtime
                        if time.time() - mtime < 120:  # Recent activity
                            status = "running"
                        else:
                            status = "failed"
                            error = task_result.get("error")
                            error_type = task_result.get("error_type")
                    except:
                        status = "running"
            else:
                status = "pending"
    # Priority 2: Check if final video exists (completed) - check both R2 and local
    elif not task_result or task_result.get("status") != "failed":
        video_exists = False
        if settings.USE_CLOUD_STORAGE:
            # Check R2 storage via database
            try:
                from web.models import VideoGenerationJob
                job = VideoGenerationJob.objects.filter(paper_id=pmid).order_by('-created_at').first()
                if job and job.final_video:
                    try:
                        video_exists = job.final_video.storage.exists(job.final_video.name)
                    except Exception:
                        pass
            except Exception:
                pass
        
        # Fallback to local filesystem check
        if not video_exists:
            video_exists = recorded.exists()
        
        if video_exists:
            status = "completed"
    
    # Priority 3: Check if log exists and determine if still running or failed
    if status != "completed" and log_path.exists():
        try:
            import time
            mtime = log_path.stat().st_mtime
            time_since_update = time.time() - mtime
            
            # Check log content for failure indicators first
            log_has_error = False
            try:
                with open(log_path, "rb") as f:
                    f.seek(max(0, f.tell() - 8192))
                    log_content = f.read().decode(errors="replace")
                    # Check for explicit failure messages
                    log_lower = log_content.lower()
                    if ("pipeline failed" in log_lower or 
                        "✗" in log_content or
                        "http error" in log_lower or
                        "bad request" in log_lower or
                        "step 'fetch-paper' failed" in log_lower):
                        log_has_error = True
                        # Extract error message
                        lines = log_content.split("\n")
                        for line in reversed(lines):
                            if (("✗" in line or "failed" in line.lower() or "error" in line.lower()) and 
                                line.strip() and
                                not line.strip().startswith("2025-")):  # Skip timestamp-only lines
                                if not error:  # Only set if we don't already have error from task_result
                                    error = line.strip()
                                    # Classify error type
                                    if "not available in pubmed central" in line.lower():
                                        error_type = "paper_not_found"
                                    elif "http error 400" in line.lower() or "bad request" in line.lower():
                                        error_type = "pipeline_error"
                                break
            except:
                pass
            
            # If log was updated recently (within 2 minutes)
            if time_since_update < 120:
                # But if log shows an error, it's failed (even if recent)
                if log_has_error:
                    status = "failed"
                    if not error_type and error:
                        # Classify error if we haven't already
                        error_lower = error.lower()
                        if "not available in pubmed central" in error_lower:
                            error_type = "paper_not_found"
                        elif "api key" in error_lower:
                            error_type = "api_key_error"
                elif current_step:
                    status = "running"
                else:
                    status = "pending"
            else:
                # Log hasn't been updated in 2+ minutes and no final video = likely failed
                status = "failed"
                # Use error from log if we found one
                if not error and log_has_error:
                    # Try to extract error from log again
                    try:
                        with open(log_path, "rb") as f:
                            f.seek(max(0, f.tell() - 8192))
                            log_content = f.read().decode(errors="replace")
                            lines = log_content.split("\n")
                            for line in reversed(lines):
                                if ("✗" in line or "failed" in line.lower()) and line.strip():
                                    error = line.strip()
                                    break
                    except:
                        pass
        except Exception:
            # If we can't check log, default based on current step
            status = "running" if current_step else "pending"
    # Priority 4: If there's a current step, it's running
    elif current_step:
        status = "running"
    # Priority 5: Otherwise pending
    else:
        status = "pending"
    
    result = {
        "current_step": current_step,
        "completed_steps": completed_steps,
        "progress_percent": progress_percent,
        "status": status,
        "total_steps": total_steps,
    }
    
    # Add error information if available
    if error:
        result["error"] = error
    if error_type:
        result["error_type"] = error_type
    
    return result


def _start_pipeline_async(pmid: str, output_dir: Path, user_id: Optional[int] = None):
    """Start the video generation pipeline using Celery task queue.

    This uses Celery to run the pipeline asynchronously, which allows
    tasks to survive server restarts and provides better error handling.
    
    Args:
        pmid: PubMed ID or paper identifier
        output_dir: Output directory path
        user_id: Optional user ID to associate with the job
    """
    # Start Celery task
    task = generate_video_task.delay(pmid, str(output_dir), user_id)
    
    # Store task ID in a file so we can check status via Celery's result backend
    task_id_file = output_dir / "task_id.txt"
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(task_id_file, "w") as f:
        f.write(task.id)
    
    # Create or update database job record
    # This ensures the job exists in the database immediately, even before the Celery task starts
    if user_id:
        try:
            from django.contrib.auth.models import User
            from web.models import VideoGenerationJob
            import logging
            db_logger = logging.getLogger(__name__)
            
            try:
                user = User.objects.get(pk=user_id)
                job, created = VideoGenerationJob.objects.get_or_create(
                    task_id=task.id,
                    defaults={
                        'user': user,
                        'paper_id': pmid,
                        'status': 'pending',
                        'progress_percent': 0,
                        'current_step': None,
                    }
                )
                if not created:
                    # Update existing job (shouldn't happen, but handle it gracefully)
                    job.status = 'pending'
                    job.progress_percent = 0
                    job.current_step = None
                    job.paper_id = pmid  # Update paper_id in case it changed
                    job.save(update_fields=['status', 'progress_percent', 'current_step', 'paper_id', 'updated_at'])
                db_logger.info(f"Created/updated job record for {pmid} with task_id {task.id}")
            except User.DoesNotExist:
                db_logger.warning(f"User {user_id} does not exist, skipping database job tracking")
            except Exception as db_error:
                db_logger.error(f"Failed to create/update job record in database: {db_error}", exc_info=True)
        except Exception as e:
            import logging
            db_logger = logging.getLogger(__name__)
            db_logger.error(f"Failed to create job record: {e}", exc_info=True)
    
    # Task is now queued and will be processed by a Celery worker
    # Status can be checked via the task ID (Celery result backend) or by reading the task_result.json file


@login_required
def upload_paper(request):
    """Simple UI to accept a PubMed ID/PMCID and start the pipeline."""
    if request.method == "POST":
        form = PaperUploadForm(request.POST)
        if form.is_valid():
            # Validate access code
            access_code = form.cleaned_data.get("access_code", "")
            try:
                if not _validate_access_code(access_code):
                    form.add_error("access_code", "Invalid access code. Please check and try again.")
                    return render(request, "upload.html", {"form": form})
            except ValueError as e:
                # Server misconfiguration - access code not set
                form.add_error(None, f"Server configuration error: {e}")
                return render(request, "upload.html", {"form": form})
            
            pmid = form.cleaned_data.get("paper_id")
            
            if not pmid:
                form.add_error("paper_id", "Please provide a PubMed ID or PMCID")
                return render(request, "upload.html", {"form": form})

            # Normalize pmid
            pmid = pmid.strip()
            
            # Skip validation for test IDs in simulation mode (e.g., TEST123, TEST456)
            # This allows testing the upload flow without validating against PubMed
            if settings.SIMULATION_MODE and pmid.upper().startswith("TEST"):
                logger.info(f"Simulation mode: Skipping paper ID validation for test ID: {pmid}")
            else:
                # Validate paper ID before starting pipeline
                is_valid, error_message = _validate_paper_id(pmid)
                if not is_valid:
                    form.add_error("paper_id", error_message)
                    return render(request, "upload.html", {"form": form})

            # Start pipeline asynchronously and redirect to status page
            output_dir = Path(settings.MEDIA_ROOT) / pmid
            user_id = request.user.id if request.user.is_authenticated else None
            _start_pipeline_async(pmid, output_dir, user_id)

            return HttpResponseRedirect(reverse("pipeline_status", args=[pmid]))
    else:
        form = PaperUploadForm()

    return render(request, "upload.html", {"form": form})


def pipeline_status(request, pmid: str):
    """Return a small status page for a running pipeline and a JSON status endpoint."""
    output_dir = Path(settings.MEDIA_ROOT) / pmid
    recorded = output_dir / "recorded.mp4"
    log_path = output_dir / "pipeline.log"

    # Try to get progress from database first
    progress = None
    try:
        from web.models import VideoGenerationJob
        
        # Try to find job for this paper_id and user (if authenticated)
        if request.user.is_authenticated:
            try:
                # Use filter().first() instead of get() to handle multiple jobs
                job = VideoGenerationJob.objects.filter(paper_id=pmid, user=request.user).order_by('-created_at').first()
                if not job:
                    # No job found, fall through to file-based check
                    pass
                else:
                    # Just refresh from database - real-time parser handles updates
                    job.refresh_from_db()
                    
                    # Check if progress is stale (for logging/debugging)
                    if job.status in ['pending', 'running']:
                        from web.progress_manager import is_progress_stale
                        if is_progress_stale(job):
                            logger.warning(
                                f"Progress appears stale for job {job.id} (paper {pmid}), "
                                f"last update: {job.progress_updated_at}"
                            )
                    
                    # Convert job to progress dict
                    completed_steps = _get_completed_steps_from_progress(job.progress_percent)
                    # If progress is 100% but status is still running, mark as completed
                    job_status = job.status
                    if job.progress_percent >= 100 and job_status in ['pending', 'running']:
                        job_status = 'completed'
                    progress = {
                        "status": job_status,
                        "current_step": job.current_step,
                        "completed_steps": completed_steps,
                        "progress_percent": job.progress_percent,
                        "total_steps": 4,
                    }
                    if job.status == 'failed':
                        progress["error"] = job.error_message
                        progress["error_type"] = job.error_type
            except VideoGenerationJob.MultipleObjectsReturned:
                # Multiple jobs found - get the most recent one
                job = VideoGenerationJob.objects.filter(paper_id=pmid, user=request.user).order_by('-created_at').first()
                if job:
                    # Just refresh from database - real-time parser handles updates
                    job.refresh_from_db()
                    
                    # Check if progress is stale (for logging/debugging)
                    if job.status in ['pending', 'running']:
                        from web.progress_manager import is_progress_stale
                        if is_progress_stale(job):
                            logger.warning(
                                f"Progress appears stale for job {job.id} (paper {pmid}), "
                                f"last update: {job.progress_updated_at}"
                            )
                    
                    completed_steps = _get_completed_steps_from_progress(job.progress_percent)
                    progress = {
                        "status": job.status,
                        "current_step": job.current_step,
                        "completed_steps": completed_steps,
                        "progress_percent": job.progress_percent,
                        "total_steps": 4,
                    }
                    if job.status == 'failed':
                        progress["error"] = job.error_message
                        progress["error_type"] = job.error_type
            except VideoGenerationJob.DoesNotExist:
                pass  # Fall through to file-based check
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Error getting progress from database: {e}")
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Error getting progress from database: {e}")

    # Fallback to file-based progress if database doesn't have it
    if progress is None:
        try:
            progress = _get_pipeline_progress(output_dir)
            # If final video exists, mark as completed
            if recorded.exists() and progress.get("progress_percent", 0) >= 100:
                progress["status"] = "completed"
        except Exception as e:
            # Fallback progress dict if _get_pipeline_progress fails
            import logging
            logger = logging.getLogger(__name__)
            logger.exception(f"Error getting pipeline progress for {pmid}: {e}")
            # If video exists, mark as completed even if we can't get progress
            if recorded.exists():
                progress = {
                    "status": "completed",
                    "current_step": None,
                    "completed_steps": ["fetch-paper", "generate-script", "generate-audio", "generate-videos"],
                    "progress_percent": 100,
                    "total_steps": 4,
                }
            else:
                progress = {
                    "status": "pending",
                    "current_step": None,
                    "completed_steps": [],
                    "progress_percent": 0,
                    "total_steps": 4,
                }
    
    # Check if video exists (cloud storage or local)
    final_video_exists, final_video_url = _check_video_exists(pmid, request.user)
    
    # Ensure status is "completed" if video exists and progress is 100%
    if final_video_exists and progress.get("progress_percent", 0) >= 100:
        progress["status"] = "completed"

    if request.GET.get("_json"):
        # JSON status endpoint - use the new progress tracking
        status = {
            "pmid": pmid,
            "exists": output_dir.exists(),
            "final_video": final_video_exists,
            "final_video_url": final_video_url,  # Use serve_video endpoint
            "status": progress.get("status", "pending"),
            "current_step": progress.get("current_step"),
            "completed_steps": progress.get("completed_steps", []),
            "progress_percent": progress.get("progress_percent", 0),
            "progress_updated_at": None,  # Add timestamp
        }
        
        # Add progress timestamp if available from job
        try:
            from web.models import VideoGenerationJob
            if request.user.is_authenticated:
                job = VideoGenerationJob.objects.filter(paper_id=pmid, user=request.user).order_by('-created_at').first()
            else:
                job = VideoGenerationJob.objects.filter(paper_id=pmid).order_by('-created_at').first()
            
            if job and job.progress_updated_at:
                status["progress_updated_at"] = job.progress_updated_at.isoformat()
        except Exception:
            pass  # Ignore errors getting timestamp
        
        # CRITICAL FIX: If status is completed or progress is 100%, ensure final_video_url is set
        if (status["status"] == "completed" or status["progress_percent"] >= 100):
            # Re-check video existence - might have been created after initial check
            final_video_exists, final_video_url = _check_video_exists(pmid, request.user)
            if final_video_exists and final_video_url:
                status["final_video_url"] = final_video_url
                status["final_video"] = True
        
        # Add error information if available
        if "error" in progress:
            status["error"] = progress["error"]
        if "error_type" in progress:
            status["error_type"] = progress["error_type"]
            # Add user-friendly error message
            status["error_message"] = _get_user_friendly_error(progress["error_type"], progress.get("error", ""))

        import json
        response = JsonResponse(status)
        response.content = json.dumps(status, indent=2)
        
        # Prevent browser caching of progress updates
        response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response['Pragma'] = 'no-cache'
        response['Expires'] = '0'
        
        return response

    # Render an HTML status page
    log_tail = ""
    if log_path.exists():
        try:
            with open(log_path, "rb") as f:
                f.seek(max(0, f.tell() - 8192))
                log_tail = f.read().decode(errors="replace")
        except Exception:
            log_tail = "(could not read log)"
    
    # Get user-friendly error message
    error_message = None
    if progress.get("error_type"):
        error_message = _get_user_friendly_error(progress["error_type"], progress.get("error", ""))

    # CRITICAL FIX: If progress is 100% or status is completed, check file again
    # Use helper function to check cloud storage or local filesystem
    final_video_exists, final_video_url = _check_video_exists(pmid, request.user)
    
    if (progress.get("progress_percent", 0) >= 100 or progress.get("status") == "completed"):
        # Re-check video existence - might have been created after initial check
        final_video_exists, final_video_url = _check_video_exists(pmid, request.user)
    
    context = {
        "pmid": pmid,
        "final_video_exists": final_video_exists,  # Use the checked value
        "final_video_url": final_video_url,
        "log_tail": log_tail,
        "progress": progress,
        "error_message": error_message,
    }

    return render(request, "status.html", context)


def pipeline_result(request, pmid: str):
    # Check if video exists (cloud storage or local)
    final_video_exists, video_url = _check_video_exists(pmid, request.user if hasattr(request, 'user') else None)
    
    if final_video_exists and video_url:
        return render(request, "result.html", {"pmid": pmid, "video_url": video_url})
    else:
        return HttpResponseRedirect(reverse("pipeline_status", args=[pmid]))


@login_required
def serve_video(request, pmid: str):
    """Serve video file from cloud storage (R2) or local filesystem."""
    from django.core.files.storage import default_storage
    from django.http import FileResponse, Http404
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # Try to get from database first (cloud storage)
        from web.models import VideoGenerationJob
        
        # Get job record - try with user filter first, then fallback to any user
        job = None
        if request.user.is_authenticated:
            job = VideoGenerationJob.objects.filter(
                paper_id=pmid, 
                user=request.user
            ).order_by('-created_at').first()
        
        # Fallback: if not found with user filter, check without user filter
        if not job:
            job = VideoGenerationJob.objects.filter(paper_id=pmid).order_by('-created_at').first()
            if job:
                logger.info(f"Video found for {pmid} but with different user. Serving anyway.")
        
        # If job has final_video FileField, serve from cloud storage
        if job and job.final_video:
            try:
                return FileResponse(
                    job.final_video.open('rb'),
                    content_type='video/mp4',
                    filename='final_video.mp4'
                )
            except Exception as e:
                logger.error(f"Error opening cloud storage file via FileField: {e}", exc_info=True)
        
        # Fallback: check final_video_path if FileField is not set
        if job and job.final_video_path and settings.USE_CLOUD_STORAGE:
            try:
                # Try to serve directly from R2 using the path
                if default_storage.exists(job.final_video_path):
                    logger.info(f"Serving video from R2 using final_video_path: {job.final_video_path}")
                    return FileResponse(
                        default_storage.open(job.final_video_path, 'rb'),
                        content_type='video/mp4',
                        filename='final_video.mp4'
                    )
                else:
                    logger.warning(f"final_video_path set but file not found in R2: {job.final_video_path}")
            except Exception as e:
                logger.error(f"Error serving video from final_video_path: {e}", exc_info=True)
        
        # Fallback: check local filesystem (for development or migration period)
        if settings.USE_CLOUD_STORAGE:
            # In production with cloud storage, if file not in cloud, it doesn't exist
            logger.error(f"Video not found in cloud storage for {pmid}. Job exists: {job is not None}, final_video: {job.final_video if job else None}, final_video_path: {job.final_video_path if job else None}")
            raise Http404("Video not found in cloud storage")
        else:
            # Local development fallback
            output_dir = Path(settings.MEDIA_ROOT) / pmid
            recorded = output_dir / "recorded.mp4"

            if recorded.exists():
                return FileResponse(
                    open(recorded, 'rb'),
                    content_type='video/mp4',
                    filename='recorded.mp4'
                )
        
        raise Http404("Video not found")
        
    except Http404:
        raise
    except Exception as e:
        logger.error(f"Error serving video: {e}", exc_info=True)
        return HttpResponse("Error serving video", status=500)


@login_required
def my_videos(request):
    """Display all videos generated by the current user."""
    try:
        from web.models import VideoGenerationJob
        
        # Get all jobs for the current user
        jobs = VideoGenerationJob.objects.filter(user=request.user).order_by('-created_at')
        
        # Add video URL and metadata for each job
        videos = []
        for job in jobs:
            try:
                video_data = {
                    'job': job,
                    'paper_id': job.paper_id or 'Unknown',
                    'status': job.status or 'pending',
                    'progress_percent': job.progress_percent or 0,
                    'current_step': job.current_step,
                    'created_at': job.created_at,
                    'completed_at': job.completed_at,
                    'error_message': job.error_message if job.status == 'failed' else None,
                    'error_type': job.error_type if job.status == 'failed' else None,
                    'video_url': None,
                    'has_video': False,
                }
                
                # Check if video file exists (cloud storage or local)
                has_file = False
                if job.paper_id:
                    try:
                        # Use helper function to check cloud storage or local filesystem
                        has_file, video_url = _check_video_exists(job.paper_id, request.user)
                        if has_file and video_url:
                            video_data['has_video'] = True
                            video_data['video_url'] = video_url
                        # Also check if job has final_video FileField (cloud storage)
                        elif job.final_video:
                            try:
                                if job.final_video.storage.exists(job.final_video.name):
                                    has_file = True
                                    video_data['has_video'] = True
                                    video_data['video_url'] = reverse('serve_video', args=[job.paper_id])
                            except Exception:
                                pass
                    except Exception as e:
                        logger.warning(f"Error checking video file for job {job.id}: {e}")
                
                # Filter out failed jobs that don't have files (likely from wiped volumes)
                # Only show failed jobs if they have files OR if they're recent (within last 7 days)
                if job.status == 'failed' and not has_file:
                    from django.utils import timezone
                    from datetime import timedelta
                    # Skip failed jobs without files that are older than 7 days
                    if job.created_at and (timezone.now() - job.created_at) > timedelta(days=7):
                        continue  # Skip this job - it's an old failed job without files
                
                videos.append(video_data)
            except Exception as e:
                logger.error(f"Error processing job {job.id if hasattr(job, 'id') else 'unknown'}: {e}")
                # Skip this job and continue with others
                continue
        
        return render(request, 'my_videos.html', {'videos': videos})
    except Exception as e:
        logger.exception(f"Error in my_videos view: {e}")
        # Return a user-friendly error page
        from django.http import HttpResponseServerError
        return render(request, 'my_videos.html', {
            'videos': [],
            'error': 'An error occurred while loading your videos. Please try again later.'
        })


def register(request):
    """User registration view."""
    if request.method == "POST":
        form = UserCreationForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            return redirect("home")
    else:
        form = UserCreationForm()
    return render(request, "registration/register.html", {"form": form})


# ============================================================================
# API ENDPOINTS
# ============================================================================

@require_http_methods(["POST"])
@csrf_exempt  # For API usage, you may want to use proper API authentication instead
def api_start_generation(request):
    """API endpoint to start video generation from a PubMed ID.
    
    POST /api/generate/
    Body (JSON):
    {
        "paper_id": "PMC10979640",  # or PMID like "33963468"
        "access_code": "your-access-code"  # Required access code
    }
    
    Returns:
    {
        "success": true,
        "paper_id": "PMC10979640",
        "status_url": "/api/status/PMC10979640/",
        "message": "Pipeline started"
    }
    """
    try:
        if request.content_type == 'application/json':
            import json
            data = json.loads(request.body)
            paper_id = (data.get("paper_id") or "").strip()
            access_code = data.get("access_code") or ""
        else:
            paper_id = (request.POST.get("paper_id") or "").strip()
            access_code = request.POST.get("access_code") or ""
        
        if not paper_id:
            return JsonResponse(
                {"success": False, "error": "paper_id is required"},
                status=400
            )
        
        # Validate access code
        try:
            if not _validate_access_code(access_code):
                return JsonResponse(
                    {"success": False, "error": "Invalid or missing access_code"},
                    status=403  # Forbidden
                )
        except ValueError as e:
            # Server misconfiguration - access code not set
            return JsonResponse(
                {"success": False, "error": f"Server configuration error: {str(e)}"},
                status=500  # Internal Server Error
            )
        
        # Validate API keys are set
        if not os.getenv("GEMINI_API_KEY"):
            return JsonResponse(
                {"success": False, "error": "GEMINI_API_KEY environment variable not set"},
                status=500
            )
        
        if not os.getenv("RUNWAYML_API_SECRET"):
            return JsonResponse(
                {"success": False, "error": "RUNWAYML_API_SECRET environment variable not set"},
                status=500
            )
        
        # Start pipeline
        output_dir = Path(settings.MEDIA_ROOT) / paper_id
        
        # Check if already running or completed
        progress = _get_pipeline_progress(output_dir)
        if progress["status"] == "running":
            return JsonResponse(
                {
                    "success": False,
                    "error": "Pipeline already running for this paper_id",
                    "status_url": f"/api/status/{paper_id}/"
                },
                status=409  # Conflict
            )
        
        # Don't restart if already completed
        if progress["status"] == "completed":
            return JsonResponse(
                {
                    "success": True,
                    "paper_id": paper_id,
                    "status_url": f"/api/status/{paper_id}/",
                    "result_url": f"/api/result/{paper_id}/",
                    "message": "Video already generated"
                }
            )
        
        # Get user ID if authenticated (API may not require auth, so this is optional)
        user_id = None
        if hasattr(request, 'user') and request.user.is_authenticated:
            user_id = request.user.id
        
        # Start the pipeline
        _start_pipeline_async(paper_id, output_dir, user_id)
        
        return JsonResponse({
            "success": True,
            "paper_id": paper_id,
            "status_url": f"/api/status/{paper_id}/",
            "result_url": f"/api/result/{paper_id}/",
            "message": "Pipeline started successfully"
        })
        
    except json.JSONDecodeError:
        return JsonResponse(
            {"success": False, "error": "Invalid JSON in request body"},
            status=400
        )
    except Exception as e:
        return JsonResponse(
            {"success": False, "error": str(e)},
            status=500
        )


@require_http_methods(["GET"])
def api_status(request, paper_id: str):
    """API endpoint to check pipeline status.
    
    GET /api/status/<paper_id>/
    
    Returns:
    {
        "paper_id": "PMC10979640",
        "status": "running",  # pending, running, completed, failed
        "current_step": "generate-videos",
        "completed_steps": ["fetch-paper", "generate-script", "generate-audio"],
        "progress_percent": 60,
        "final_video_url": "/media/PMC10979640/final_video.mp4" or null,
        "log_tail": "last 8KB of log file"
    }
    """
    output_dir = Path(settings.MEDIA_ROOT) / paper_id
    
    # Try to get progress from database first
    progress = None
    try:
        from web.models import VideoGenerationJob
        
        # Try to find most recent job for this paper_id
        # If user is authenticated, prefer their job
        if hasattr(request, 'user') and request.user.is_authenticated:
            try:
                job = VideoGenerationJob.objects.filter(paper_id=paper_id, user=request.user).order_by('-created_at').first()
            except:
                job = None
        else:
            job = None
        
        if not job:
            # Try to find any job for this paper_id
            try:
                job = VideoGenerationJob.objects.filter(paper_id=paper_id).order_by('-created_at').first()
            except:
                job = None
        
        if job:
            # Just refresh from database - real-time parser handles updates
            job.refresh_from_db()
            
            # Check if progress is stale (for logging/debugging)
            if job.status in ['pending', 'running']:
                from web.progress_manager import is_progress_stale
                if is_progress_stale(job):
                    logger.warning(
                        f"Progress appears stale for job {job.id} (paper {paper_id}), "
                        f"last update: {job.progress_updated_at}"
                    )
            
            # Convert job to progress dict
            completed_steps = _get_completed_steps_from_progress(job.progress_percent)
            
            # Check if video exists (R2 or local) - if it does and progress is 100%, mark as completed
            final_video_exists, final_video_url = _check_video_exists(paper_id, request.user if hasattr(request, 'user') and request.user.is_authenticated else None)
            job_status = job.status
            if final_video_exists and job.progress_percent >= 100:
                job_status = 'completed'
            
            progress = {
                "status": job_status,
                "current_step": job.current_step,
                "completed_steps": completed_steps,
                "progress_percent": job.progress_percent,
                "progress_updated_at": job.progress_updated_at.isoformat() if job.progress_updated_at else None,
            }
            if job.status == 'failed':
                progress["error"] = job.error_message
                progress["error_type"] = job.error_type
            
            # Add video URL if available
            if final_video_exists and final_video_url:
                progress["final_video_url"] = final_video_url
            elif job.final_video:
                # Job has final_video FileField, try to get URL
                try:
                    if job.final_video.storage.exists(job.final_video.name):
                        progress["final_video_url"] = reverse("serve_video", args=[paper_id])
                except Exception:
                    pass
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Error getting progress from database in API: {e}")
    
    # Fallback to file-based progress
    if progress is None:
        progress = _get_pipeline_progress(output_dir)
    
    recorded = output_dir / "recorded.mp4"
    final_video_url = None
    if recorded.exists():
        final_video_url = f"{settings.MEDIA_URL.rstrip('/')}/{paper_id}/recorded.mp4"
    
    # Get log tail
    log_path = output_dir / "pipeline.log"
    log_tail = ""
    if log_path.exists():
        try:
            with open(log_path, "rb") as f:
                f.seek(max(0, f.tell() - 8192))
                log_tail = f.read().decode(errors="replace")
        except Exception:
            log_tail = "(could not read log)"
    
    response = {
        "paper_id": paper_id,
        "status": progress["status"],
        "current_step": progress["current_step"],
        "completed_steps": progress["completed_steps"],
        "progress_percent": progress["progress_percent"],
        "progress_updated_at": progress.get("progress_updated_at"),
        "final_video_url": final_video_url,
        "log_tail": log_tail,
    }
    
    json_response = JsonResponse(response)
    
    # Prevent browser caching of progress updates
    json_response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    json_response['Pragma'] = 'no-cache'
    json_response['Expires'] = '0'
    
    return json_response


@require_http_methods(["GET"])
def api_result(request, paper_id: str):
    """API endpoint to get the final video result.
    
    GET /api/result/<paper_id>/
    
    Returns:
    {
        "paper_id": "PMC10979640",
        "success": true,
        "video_url": "/video/PMC10979640/",
        "status": "completed"
    }
    or
    {
        "paper_id": "PMC10979640",
        "success": false,
        "error": "Video not ready yet",
        "status": "running",
        "status_url": "/api/status/PMC10979640/"
    }
    """
    # Check if video exists (cloud storage or local)
    final_video_exists, video_url = _check_video_exists(paper_id, request.user if hasattr(request, 'user') and request.user.is_authenticated else None)
    
    if final_video_exists and video_url:
        return JsonResponse({
            "paper_id": paper_id,
            "success": True,
            "video_url": video_url,  # Use serve_video endpoint
            "status": "completed",
            "progress_percent": 100,
        })
    else:
        # Get progress for status info
        output_dir = Path(settings.MEDIA_ROOT) / paper_id
        progress = _get_pipeline_progress(output_dir)
        
        return JsonResponse({
            "paper_id": paper_id,
            "success": False,
            "error": "Video not ready yet",
            "status": progress["status"],
            "progress_percent": progress["progress_percent"],
            "status_url": f"/api/status/{paper_id}/",
        }, status=202)  # Accepted but not ready


def analytics_endpoint(request):
    """
    Standardized analytics endpoint at /e9ec8bb (first 7 chars of sha1("hidden-hill")).
    
    This endpoint:
    - Is publicly accessible (no authentication required)
    - Displays a list of all team member nicknames
    - Includes a clickable button with id="abtest" that alternates between variants:
      - Variant A: "kudos"
      - Variant B: "thanks"
    - Tracks analytics for impressions and clicks
    """
    import hashlib
    import uuid
    from web.models import ABTestEvent
    
    # Team member nicknames
    TEAM_NICKNAMES = [
        "charming-leopard",
        "courageous-mallard",
        "Vicent (ID unknown)",
        "Kenza (ID unknown)",
        "Omar (ID unknown)",
        "Nora (ID unknown)",
        # Add more team member nicknames here as needed
    ]
    
    # Get or create session ID from cookie
    session_id = request.COOKIES.get('analytics_session_id')
    if not session_id:
        session_id = str(uuid.uuid4())
    
    # Assign A/B test variant (50/50 split)
    # Use session_id hash for consistent assignment per session
    variant_hash = int(hashlib.md5(session_id.encode()).hexdigest(), 16)
    variant = 'A' if (variant_hash % 2 == 0) else 'B'
    
    button_text = 'kudos' if variant == 'A' else 'thanks'
    
    # Track impression
    try:
        ABTestEvent.objects.create(
            event_type='impression',
            variant=variant,
            session_id=session_id,
            ip_address=_get_client_ip(request),
            user_agent=request.META.get('HTTP_USER_AGENT', '')[:500],  # Limit length
        )
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Failed to track impression: {e}")
    
    context = {
        'team_nicknames': TEAM_NICKNAMES,
        'variant': variant,
        'button_text': button_text,
        'session_id': session_id,
    }
    
    response = render(request, 'analytics.html', context)
    
    # Set session cookie if not already set
    if not request.COOKIES.get('analytics_session_id'):
        response.set_cookie('analytics_session_id', session_id, max_age=365*24*60*60)  # 1 year
    
    return response


def _get_client_ip(request):
    """Get client IP address from request."""
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip


@require_http_methods(["POST"])
@csrf_exempt
def analytics_track_click(request):
    """
    API endpoint to track button clicks for A/B testing.
    
    POST /analytics/track-click/
    Body (JSON):
    {
        "session_id": "uuid",
        "variant": "A" or "B"
    }
    """
    import json
    from web.models import ABTestEvent
    
    try:
        if request.content_type == 'application/json':
            data = json.loads(request.body)
        else:
            data = request.POST
        
        session_id = data.get('session_id', '')
        variant = data.get('variant', '').upper()
        
        if not session_id or variant not in ['A', 'B']:
            return JsonResponse(
                {'success': False, 'error': 'Invalid session_id or variant'},
                status=400
            )
        
        # Track click
        ABTestEvent.objects.create(
            event_type='click',
            variant=variant,
            session_id=session_id,
            ip_address=_get_client_ip(request),
            user_agent=request.META.get('HTTP_USER_AGENT', '')[:500],
        )
        
        return JsonResponse({'success': True})
        
    except json.JSONDecodeError:
        return JsonResponse(
            {'success': False, 'error': 'Invalid JSON'},
            status=400
        )
    except Exception as e:
        return JsonResponse(
            {'success': False, 'error': str(e)},
            status=500
        )