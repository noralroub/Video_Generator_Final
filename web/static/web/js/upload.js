// Upload Page Interactivity

document.addEventListener('DOMContentLoaded', function() {
    // Elements
    const form = document.getElementById('paper-upload-form');
    const submitBtn = form ? form.querySelector('button[type="submit"]') : null;
    const textInputs = form ? form.querySelectorAll('input[type="text"]') : [];

    // Form validation
    if (form) {
        form.addEventListener('submit', function(e) {
            const textInputValue = Array.from(textInputs).some(input => input.value.trim());

            if (!textInputValue) {
                e.preventDefault();
                showAlert('Please provide a PubMed ID or PMC ID.', 'error');
            }
        });
    }

    // Input focus effects
    textInputs.forEach(input => {
        input.addEventListener('focus', function() {
            this.parentElement.classList.add('focused');
        });

        input.addEventListener('blur', function() {
            this.parentElement.classList.remove('focused');
        });
    });

    // Button loading state
    if (submitBtn && form) {
        form.addEventListener('submit', function(e) {
            const textInputValue = Array.from(textInputs).some(input => input.value.trim());

            if (textInputValue) {
                submitBtn.disabled = true;
                submitBtn.classList.add('loading');
                const originalText = submitBtn.textContent;
                submitBtn.textContent = 'Processing...';

                // Reset after 3 seconds if form doesn't submit
                setTimeout(() => {
                    submitBtn.disabled = false;
                    submitBtn.classList.remove('loading');
                    submitBtn.textContent = originalText;
                }, 3000);
            }
        });
    }

    // Show alert messages
    function showAlert(message, type) {
        const alertDiv = document.createElement('div');
        alertDiv.className = `alert alert-${type}`;
        alertDiv.textContent = message;

        const formGroup = document.querySelector('.form-group');
        if (formGroup) {
            formGroup.parentElement.insertBefore(alertDiv, formGroup);

            // Auto remove after 5 seconds
            setTimeout(() => {
                alertDiv.style.animation = 'slideUp 0.3s ease-out forwards';
                setTimeout(() => alertDiv.remove(), 300);
            }, 5000);
        }
    }

    // Smooth scroll to errors
    const errorMessages = document.querySelectorAll('.errorlist');
    if (errorMessages.length > 0) {
        errorMessages.forEach(error => {
            error.scrollIntoView({ behavior: 'smooth', block: 'center' });
        });
    }

    // Add smooth animations to form groups
    const formGroups = document.querySelectorAll('.form-group');
    formGroups.forEach((group, index) => {
        group.style.animation = `slideUp 0.4s ease-out ${index * 0.1}s backwards`;
    });
});
