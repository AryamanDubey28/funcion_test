import azure.functions as func
from datetime import date, datetime, timezone, timedelta
import logging
import os
from azure.communication.email import EmailClient
import psycopg2
from psycopg2.extras import DictCursor
import json
from typing import Optional, Dict, List

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 */24 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False)
def funcmulticloud(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    conn = None
    cursor = None
    
    try:
        email_connection_string = "endpoint=https://nhsbtcomserv.unitedstates.communication.azure.com/;accesskey=CwbcivnMpdKglt79mjd9L4iAfrt8dWv9ZwsCc0PHHNGSyoYnDw0dJQQJ99AKACULyCpuClzqAAAAAZCSfgfx"
        sender_email = "DoNotReply@9e695a82-77ce-49ff-b39c-6ccdf369f894.azurecomm.net"
        email_client = EmailClient.from_connection_string(email_connection_string)
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)
        
        # Get all users
        try:
            cursor.execute("SELECT id, email, subscription_id FROM users")
            users = cursor.fetchall()
        except psycopg2.Error as e:
            logging.error(f"Error fetching users: {str(e)}")
            raise
        
        for user in users:
            try:
                process_user_resources(cursor, conn, user, email_client, sender_email)
            except Exception as e:
                logging.error(f"Error processing user {user['id']}: {str(e)}")
                # Continue with next user instead of failing completely
                continue
                
    except Exception as e:
        logging.error(f"Critical error in main function: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_db_connection():
    """Create a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=os.environ.get('DB_NAME', 'postgres'),
            user=os.environ.get('DB_USER', 'psqladmin'),
            password=os.environ.get('DB_PASSWORD', 'Ary282322'),
            host=os.environ.get('DB_HOST', 'nhsbt-users.postgres.database.azure.com'),
            port=os.environ.get('DB_PORT', '5432')
        )
        return conn
    except psycopg2.Error as e:
        logging.error(f"Database connection error: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error connecting to database: {str(e)}")
        raise

def should_send_notification(last_notification_date: Optional[datetime], days_until_deprecation: int) -> bool:
    """
    Determine if a notification should be sent based on time until deprecation
    and when the last notification was sent.
    """
    if last_notification_date is None:
        return True
    
    current_time = datetime.now(timezone.utc)
    if not last_notification_date.tzinfo:
        last_notification_date = last_notification_date.replace(tzinfo=timezone.utc)
    
    days_since_last_notification = (current_time - last_notification_date).days
    
    if days_until_deprecation <= 14:  # Within 2 weeks
        return days_since_last_notification >= 1
    elif days_until_deprecation <= 30:  # Within 1 month
        return days_since_last_notification >= 7
    elif days_until_deprecation <= 90:  # Within 3 months
        return days_since_last_notification >= 14
    else:  # More than 3 months
        return days_since_last_notification >= 30


def process_user_resources(cursor: DictCursor, conn: psycopg2.extensions.connection, 
                         user: Dict, email_client: EmailClient, sender_email: str) -> None:
    """Process resources for a single user."""
    user_id = user['id']
    email = user['email']
    
    try:
        cursor.execute("""
            SELECT 
                ar.id,
                ar.resource_name,
                ar.resource_type,
                ar.provider,
                ar.deprecation_date::date,
                ar.replacement_service,
                (
                    SELECT MAX(created_at AT TIME ZONE 'UTC')
                    FROM notifications n
                    WHERE n.resource_id = ar.id
                    AND n.user_id = %s
                ) as last_notification_date
            FROM azure_resources ar
            WHERE ar.user_id = %s
            AND ar.deprecation_date IS NOT NULL
            AND ar.deprecation_date::date BETWEEN CURRENT_DATE AND (CURRENT_DATE + INTERVAL '90 days')
        """, (user_id, user_id))
        
        all_resources = []
        processed_resources = set()
        
        for resource in cursor.fetchall():
            if resource['id'] not in processed_resources:
                processed_resources.add(resource['id'])
                days_until_deprecation = (resource['deprecation_date'] - date.today()).days
                
                if should_send_notification(resource['last_notification_date'], days_until_deprecation):
                    all_resources.append({
                        'id': resource['id'],
                        'name': resource['resource_name'],
                        'type': f"{resource['provider']}/{resource['resource_type']}",
                        'deprecation_date': resource['deprecation_date'],
                        'replacement_service': resource['replacement_service'],
                        'days_until_deprecation': days_until_deprecation
                    })
        
        if all_resources:
            resources_by_urgency = {
                'critical': [],
                'urgent': [],
                'warning': []
            }
            
            for resource in all_resources:
                if resource['days_until_deprecation'] <= 14:
                    resources_by_urgency['critical'].append(resource)
                elif resource['days_until_deprecation'] <= 30:
                    resources_by_urgency['urgent'].append(resource)
                else:
                    resources_by_urgency['warning'].append(resource)
            
            message = {
                "content": {
                    "subject": f"Azure Resources Deprecation Alert - {len(all_resources)} Resources Requiring Action",
                    "html": create_email_content(resources_by_urgency)
                },
                "recipients": {
                    "to": [{"address": email}]
                },
                "senderAddress": sender_email
            }
            
            try:
                poller = email_client.begin_send(message)
                result = poller.result()
                store_portal_notification(cursor, user_id, all_resources)
                conn.commit()
                logging.info(f"Successfully sent email and stored notifications for user {user_id}")
            except Exception as e:
                conn.rollback()
                logging.error(f"Failed to send email to {email}: {str(e)}")
                raise
                
    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"Database error processing user {user_id}: {str(e)}")
        raise
    except Exception as e:
        conn.rollback()
        logging.error(f"Unexpected error processing user {user_id}: {str(e)}")
        raise

def create_email_content(resources_by_urgency: Dict[str, List[Dict]]) -> str:
    """Create formatted email content with resources grouped by urgency."""
    content = """
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 100%; margin: 0; padding: 0; background-color: #ffffff;">
        <!-- Header -->
        <div style="background-color: #2b579a; padding: 24px; text-align: center; border-radius: 8px 8px 0 0;">
            <h1 style="color: #ffffff; margin: 0; font-size: 24px; font-weight: 500;">Azure Resources Deprecation Alert</h1>
        </div>
        
        <!-- Main Content Container -->
        <div style="padding: 32px 24px;">
    """
    
    urgency_settings = {
        'critical': {
            'title': 'Critical: Action Required Within 2 Weeks',
            'color': '#dc3545',
            'note': 'These resources will be deprecated within 2 weeks. Immediate action is required.',
            'bg_color': '#fff5f5'
        },
        'urgent': {
            'title': 'Urgent: Action Required Within 1 Month',
            'color': '#ff9800',
            'note': 'These resources will be deprecated within 1 month. Please plan migration soon.',
            'bg_color': '#fff8e1'
        },
        'warning': {
            'title': 'Warning: Action Required Within 3 Months',
            'color': '#0288d1',
            'note': 'These resources will be deprecated within 3 months. Please begin planning their migration.',
            'bg_color': '#e1f5fe'
        }
    }
    
    for urgency, resources in resources_by_urgency.items():
        if not resources:
            continue
            
        settings = urgency_settings[urgency]
        content += f"""
            <div style="margin-bottom: 40px; background-color: {settings['bg_color']}; border-radius: 8px; padding: 24px; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">
                <h2 style="color: {settings['color']}; margin: 0 0 16px 0; font-size: 20px; font-weight: 500;">
                    {settings['title']}
                </h2>
                <p style="color: #4a5568; margin: 0 0 24px 0; font-size: 15px; line-height: 1.5;">
                    {settings['note']}
                </p>
                
                <!-- Table Container with Horizontal Scroll -->
                <div style="background-color: #ffffff; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); overflow-x: auto; max-width: 100%; -webkit-overflow-scrolling: touch;">
                    <table style="border-collapse: collapse; width: 100%; margin: 0; font-size: 14px; min-width: 800px;">
                        <thead>
                            <tr style="background-color: #f8fafc;">
                                <th style="padding: 16px; text-align: left; border-bottom: 2px solid #e2e8f0; color: #4a5568; font-weight: 500; white-space: nowrap;">Resource Name</th>
                                <th style="padding: 16px; text-align: left; border-bottom: 2px solid #e2e8f0; color: #4a5568; font-weight: 500; white-space: nowrap;">Resource Type</th>
                                <th style="padding: 16px; text-align: left; border-bottom: 2px solid #e2e8f0; color: #4a5568; font-weight: 500; white-space: nowrap;">Deprecation Date</th>
                                <th style="padding: 16px; text-align: left; border-bottom: 2px solid #e2e8f0; color: #4a5568; font-weight: 500; white-space: nowrap;">Days Remaining</th>
                                <th style="padding: 16px; text-align: left; border-bottom: 2px solid #e2e8f0; color: #4a5568; font-weight: 500; white-space: nowrap;">Recommended Replacement</th>
                            </tr>
                        </thead>
                        <tbody>
        """
        
        for i, resource in enumerate(resources):
            row_style = 'background-color: #ffffff;' if i % 2 == 0 else 'background-color: #f8fafc;'
            content += f"""
                <tr style="{row_style}">
                    <td style="padding: 16px; border-bottom: 1px solid #e2e8f0; color: #4a5568; min-width: 150px;">{resource['name']}</td>
                    <td style="padding: 16px; border-bottom: 1px solid #e2e8f0; color: #4a5568; min-width: 200px;">{resource['type']}</td>
                    <td style="padding: 16px; border-bottom: 1px solid #e2e8f0; color: #4a5568; white-space: nowrap;">{resource['deprecation_date']}</td>
                    <td style="padding: 16px; border-bottom: 1px solid #e2e8f0; color: #4a5568; white-space: nowrap;">{resource['days_until_deprecation']} days</td>
                    <td style="padding: 16px; border-bottom: 1px solid #e2e8f0; color: #4a5568; min-width: 200px;">{resource['replacement_service'] or 'N/A'}</td>
                </tr>
            """
        
        content += """
                        </tbody>
                    </table>
                </div>
            </div>
        """
    
    content += """
            <!-- Important Information Box -->
            <div style="background-color: #f8fafc; border-left: 4px solid #2b579a; border-radius: 4px; padding: 24px; margin: 32px 0;">
                <h3 style="color: #2b579a; margin: 0 0 16px 0; font-size: 18px; font-weight: 500;">Important Information</h3>
                <p style="color: #4a5568; margin: 0; line-height: 1.6; font-size: 15px;">
                    Once you have migrated or updated these resources, please sign in to the Multicloud Deprecation Tracker 
                    portal. Your resource inventory will automatically refresh, and these notifications will cease for the 
                    updated resources.
                </p>
            </div>

            <!-- Footer -->
            <div style="border-top: 1px solid #e2e8f0; margin-top: 32px; padding-top: 24px;">
                <p style="color: #718096; margin: 0; font-size: 14px; line-height: 1.6; text-align: center;">
                    This is an automated message from the Multicloud Deprecation Tracker.<br>
                    If you need assistance with your resource migration or have questions about this notification,<br>
                    please contact your system administrator or cloud services team.
                </p>
            </div>
        </div>
    </div>
    """
    
    return content

def store_portal_notification(cursor: DictCursor, user_id: int, resources: List[Dict]) -> None:
    """Store notifications in Postgres notifications table."""
    try:
        for resource in resources:
            message = (
                f"Resource '{resource['name']}' ({resource['type']}) "
                f"will be deprecated in {resource['days_until_deprecation']} days. "
                f"Recommended replacement: {resource['replacement_service'] or 'N/A'}"
            )
            
            cursor.execute("""
                INSERT INTO notifications (user_id, resource_id, message, status, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                user_id,
                resource['id'],
                message,
                'Unread',
                datetime.now(timezone.utc)
            ))
            
    except psycopg2.Error as e:
        logging.error(f"Database error while storing notification: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error while storing notification: {str(e)}")
        raise
