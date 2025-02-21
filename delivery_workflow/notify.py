import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from email.mime.base import MIMEBase
from email import encoders


def send_email_notification_apex(
    sender_email: str,
    sender_password: str,
    recipient_emails: list[str],
    json_folder_url: str,
    collab_folder_url: str,
    sheet_url: str,
    batch: str,
    project: str
):
    """
    Sends an email notification that includes links to:
      - The JSON folder
      - The Collab folder
      - The Google Sheet
    along with basic project info.

    Args:
        sender_email (str): Sender's email address (Gmail).
        sender_password (str): Sender's email password or app password (Gmail).
        recipient_emails (list[str]): List of recipient email addresses.
        json_folder_url (str): URL of the JSON folder in Google Drive.
        collab_folder_url (str): URL of the Collab (notebook) folder in Google Drive.
        sheet_url (str): URL of the Google Sheet with issues/links.
        batch (str): Name or identifier for this batch.
        project (str): Name of the project/category for the newly uploaded items.
    """

    subject = f"Upload Notification: New {project} Folders Created (Batch: {batch})"
    body = f"""
    <html>
    <body>
        <p>Hello Team,</p>
        <p>The following folders have been successfully created in Google Drive and populated with files:</p>
        <ul>
            <li><b>Json Folder:</b> <a href="{json_folder_url}">{project} JSON Files</a></li>
            <li><b>Collabs Folder:</b> <a href="{collab_folder_url}">{project} Collabs Files</a></li>
            <li><b>Google Sheet (Delivery Summary):</b> <a href="{sheet_url}">View Sheet</a></li>
        </ul>
        <p>Batch: <b>{batch}</b></p>
        <p>Best regards,<br>Your Automation Script</p>
    </body>
    </html>
    """

    # Set up the email with HTML content
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(recipient_emails)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))

    # Attempt to send the email via SMTP (Gmail)
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        print(f"Email sent successfully to {recipient_emails}")
    except Exception as e:
        print(f"Failed to send email: {e}")


def send_email_notification_json_only(
    sender_email: str,
    sender_password: str,
    recipient_emails: list[str],
    json_folder_url: str,
    batch: str,
    project: str
):
    """
    Sends an email notification that includes links to:
      - The JSON folder
      - The Collab folder
      - The Google Sheet
    along with basic project info.

    Args:
        sender_email (str): Sender's email address (Gmail).
        sender_password (str): Sender's email password or app password (Gmail).
        recipient_emails (list[str]): List of recipient email addresses.
        json_folder_url (str): URL of the JSON folder in Google Drive.
        collab_folder_url (str): URL of the Collab (notebook) folder in Google Drive.
        sheet_url (str): URL of the Google Sheet with issues/links.
        batch (str): Name or identifier for this batch.
        project (str): Name of the project/category for the newly uploaded items.
    """

    subject = f"Upload Notification: New {project} Folders Created (Batch: {batch})"
    body = f"""
    <html>
    <body>
        <p>Hello Team,</p>
        <p>The following folders have been successfully created in Google Drive and populated with files:</p>
        <ul>
            <li><b>Json Folder:</b> <a href="{json_folder_url}">{project} JSON Files</a></li>
        </ul>
        <p>Batch: <b>{batch}</b></p>
        <p>Best regards,<br>Your Automation Script</p>
    </body>
    </html>
    """

    # Set up the email with HTML content
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(recipient_emails)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))

    # Attempt to send the email via SMTP (Gmail)
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        print(f"Email sent successfully to {recipient_emails}")
    except Exception as e:
        print(f"Failed to send email: {e}")



def send_lwc_issue_email_notification(
    sender_email: str,
    sender_password: str,
    recipient_emails: list[str],
    sheet_url: str,
    batch: str,
    project: str,
    file_type: str
):
    """
    Sends an email notification
    """

    subject = f"Upload Notification: Issues while Processing {file_type} For {project} in {batch} batch"
    body = f"""
    <html>
    <body>
        <p>Hello Team,</p>
        <p>The following collab links have issues:</p>
        <ul>
            <li><b>Google Sheet (Issues &amp; Links):</b> <a href="{sheet_url}">View Sheet</a></li>
        </ul>
        <p>Batch: <b>{batch}</b></p>
        <p>Best regards,<br>Your Automation Script</p>
    </body>
    </html>
    """

    # Set up the email with HTML content
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(recipient_emails)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))

    # Attempt to send the email via SMTP (Gmail)
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        print(f"Email sent successfully to {recipient_emails}")
    except Exception as e:
        print(f"Failed to send email: {e}")

def send_email_notification(
    sender_email: str,
    sender_password: str,
    recipient_emails: list[str],
    drive_folder_url: str,
    sheet_url: str,
    batch: str,
    project: str
):
    """
    Sends an email notification with details about the uploaded folder and Google Sheet.

    Args:
        sender_email: The sender's email address.
        sender_password: The sender's email password or app password.
        recipient_email: The recipient's email address.
        drive_folder_url: URL of the created folder in Google Drive.
        sheet_url: URL of the Google Sheet with issues and links.
    """
    subject = f"Upload Notification: New {project} Folder Created"
    body = f"""
    <html>
    <body>
        <p>Hello Team,</p>
        <p>A new folder has been successfully created in Google Drive and populated with files. Here are the details:</p>
        <ul>
            <li><b>Folder Name and Link:</b> <a href="{drive_folder_url}">{project} Folder</a></li>
            <li><b>Google Sheet (Issues & Links):</b> <a href="{sheet_url}">View Sheet</a></li>
        </ul>
        <p>Best regards,<br>Your Automation Script</p>
    </body>
    </html>
    """

    # Set up the email
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(recipient_emails)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))

    # Send the email
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        print(f"Email sent successfully to {recipient_emails}")
    except Exception as e:
        print(f"Failed to send email: {e}")





def send_email_notification_with_zip_folder(
    sender_email: str,
    sender_password: str,
    recipient_emails: list[str],
    sheet_url: str,
    batch: str,
    project: str,
    zip_file_path: str
):
    """
    Sends an email notification with a zip file attachment and Google Sheet details.

    Args:
        sender_email (str): The sender's email address.
        sender_password (str): The sender's email password or app password.
        recipient_emails (list[str]): List of recipient email addresses.
        sheet_url (str): URL of the Google Sheet with issues and links.
        batch (str): Batch name for reference.
        project (str): Project name for reference.
        zip_file_path (str): Path to the zip file to be attached.
    """
    subject = f"Upload Notification: {project} - {batch} Data Update"
    body = f"""
    <html>
    <body>
        <p>Hello Team,</p>
        <p>The data processing for <b>{project}</b> (Batch: <b>{batch}</b>) has been completed. Please find the attached zip file along with the updated Google Sheet:</p>
        <ul>
            <li><b>Google Sheet (Delivery Summary):</b> <a href="{sheet_url}">View Sheet</a></li>
        </ul>
        <p><b>Tabs in the Google Sheet:</b></p>
        <ul>
            <li><b>Delivery:</b> Contains the delivery batch summary.</li>
        </ul>
        <p>Best regards,<br>Your Automation Script</p>
    </body>
    </html>
    """

    # Set up the email
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(recipient_emails)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))

    # Attach the zip file
    try:
        with open(zip_file_path, "rb") as attachment:
            part = MIMEBase("application", "zip")
            part.set_payload(attachment.read())

        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(zip_file_path)}")
        msg.attach(part)
    except Exception as e:
        print(f"❌ Failed to attach zip file: {e}")

    # Send the email
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)

        print(f"✅ Email sent successfully to {recipient_emails}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
