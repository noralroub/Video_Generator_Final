from django import forms
from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth.models import User


class EmailRequiredUserCreationForm(UserCreationForm):
    """Registration form that requires a unique email address."""

    email = forms.EmailField(
        required=True,
        label="Email address",
        help_text="Required. Used for password reset emails.",
        widget=forms.EmailInput(attrs={"autocomplete": "email"}),
    )

    class Meta(UserCreationForm.Meta):
        model = User
        fields = ("username", "email", "password1", "password2")

    def clean_email(self):
        email = self.cleaned_data["email"].strip().lower()
        if User.objects.filter(email__iexact=email).exists():
            raise forms.ValidationError("An account with this email already exists.")
        return email

    def save(self, commit=True):
        user = super().save(commit=False)
        user.email = self.cleaned_data["email"]
        if commit:
            user.save()
        return user


class PaperUploadForm(forms.Form):
    """Simple form to upload a paper (PMID or PMCID)."""

    paper_id = forms.CharField(
        required=True,
        label="PubMed ID or PMCID",
        help_text="Enter a PMID (e.g. 33963468) or PMCID (e.g. PMC10979640)",
        widget=forms.TextInput(attrs={"autocomplete": "off"}),
    )

    access_code = forms.CharField(
        required=True,
        label="Access Code",
        help_text="Enter the access code to generate videos",
        widget=forms.PasswordInput(attrs={
            "placeholder": "Enter access code",
            "autocomplete": "one-time-code",  # Designed for codes, prevents password manager autofill
            "data-form-type": "other",
            "data-lpignore": "true",  # LastPass ignore
            "data-1p-ignore": "true",  # 1Password ignore
            "data-bwignore": "true",  # Bitwarden ignore
            "data-dashlane-ignore": "true",  # Dashlane ignore
            "readonly": "readonly",  # Will be removed by JavaScript on focus
            "onfocus": "this.removeAttribute('readonly')",
            "onclick": "this.removeAttribute('readonly')",
            "onkeydown": "this.removeAttribute('readonly')",
        }),
    )

    def __init__(self, *args, require_access_code=True, **kwargs):
        super().__init__(*args, **kwargs)
        if not require_access_code:
            self.fields["access_code"].required = False
            self.fields["access_code"].help_text = "Not required for this deployment."
