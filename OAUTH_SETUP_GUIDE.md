# Google Cloud OAuth Setup Guide

To enable the "Login with Google" feature, you need to create an OAuth 2.0 Client ID in your Google Cloud Project.

### Step 1: Configure the OAuth Consent Screen
1.  Go to the [APIs & Services > OAuth consent screen](https://console.cloud.google.com/apis/credentials/consent) page in the Google Cloud Console.
2.  Choose **User Type**:
    - **Internal**: If you are using a Google Workspace account and only want people in your org to login.
    - **External**: If you want to allow any Google account (requires more configuration later).
3.  Click **Create**.
4.  Fill in the **App information**:
    - **App name**: e.g., "Agentic Data Steward"
    - **User support email**: Your email.
    - **Developer contact info**: Your email.
5.  Click **Save and Continue**.
6.  **Scopes**: Click **Add or Remove Scopes** and add:
    - `.../auth/bigquery`
    - `.../auth/cloud-platform`
    - `.../auth/userinfo.email`
7.  Click **Save and Continue** through the rest of the steps.

### Step 2: Create OAuth Client ID
1.  Go to the [APIs & Services > Credentials](https://console.cloud.google.com/apis/credentials) page.
2.  Click **Create Credentials** > **OAuth client ID**.
3.  Select **Application type**: **Web application**. 
4.  **Name**: e.g., "Data Steward Web Client".
5.  **Authorized redirect URIs**:
    - Click **ADD URI**.
    - Enter: `http://localhost:7860/google_callback`
    > [!IMPORTANT]
    > This MUST match exactly. It must be `google_callback`.
6.  Click **Create**.
7.  A dialog will show your Client ID and Client Secret. 
8.  Copy these into your `.env` file for:
    - `GOOGLE_CLIENT_ID`
    - `GOOGLE_CLIENT_SECRET`
    - **Ensure** `GOOGLE_REDIRECT_URI=http://localhost:7860/google_callback` is also set in `.env`.

### Step 3: Usage
Once configured, restart the application:
```bash
python3 ui/gradio_app.py
```
The app will now show a "Login with Google" button.
