# pip install pypiwin32
# pip install openai

import win32com.client
import requests
from urllib.parse import quote
import pandas as pd
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import re
import numpy as np
from datetime import datetime
import time
from openai import OpenAI, RateLimitError

class EmailMetrics:
    def __init__(self,\
                 client_id_file_path=None,\
                 employee_dept_mapping_file_path=None,\
                 tenant_id=None,\
                 graph_client_id=None,\
                 graph_client_secret=None,\
                 openai_key=None,\
                 openai_request_rate_limit_per_min=None,\
                 openai_request_delay=None,\
                 openai_model=None,\
                db_username=None,\
                db_password=None,\
                db_host=None,\
                db_port=None,\
                db_name=None):
        self.client_id_file_path = client_id_file_path if client_id_file_path is not None else r"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        self.employee_dept_mapping_file_path = employee_dept_mapping_file_path if employee_dept_mapping_file_path is not None else r"xxxxxxxxxxxxxxxxxxxxxxx"
        self.tenant_id = tenant_id if tenant_id is not None else 'xxxxxxxxxxxxxxxxxxxxxxxx'
        self.openai_key = openai_key if openai_key is not None else "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        self.graph_client_id = graph_client_id if graph_client_id is not None else "xxxxxxxxxxxxxxxxxxxxxxxx"
        self.graph_client_secret = graph_client_secret if graph_client_id is not None else "xxxxxxxxxxxxxxxxxxxxxxx"
        #DB credentials
        self.db_username = db_username if db_username is not None else 'postgres'
        self.db_password = db_password if db_password is not None else 'root'
        self.db_host = db_host if db_host is not None else 'localhost'
        self.db_port = db_port if db_port is not None else '5432'
        self.db_name = db_name if db_name is not None else 'outlook_reporting' 
        print("Starting script initialization and configuration...")         
        print("Connecting to Outlook...")
        self.outlookObj = win32com.client.Dispatch("Outlook.Application")
        print(f"Connected to Outlook {self.outlookObj.Version}.")
        # Get Graph API access token
        print("Fetching access token from graph API...")        
        token_url = f'https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token'
        token_payload = {
            'grant_type': 'client_credentials',
            'client_id': self.graph_client_id,
            'client_secret': self.graph_client_secret,
            'scope': 'https://graph.microsoft.com/.default'
        }

        token_response = requests.post(token_url, data=token_payload)
        self.graph_access_token = token_response.json().get('access_token')
        print("Successfully fetched graph access token!")
        print("Loading OpenAI config...")               
        self.openai_request_rate_limit_per_min = openai_request_rate_limit_per_min if openai_request_rate_limit_per_min is not None else 500
        self.openai_request_delay = openai_request_delay if openai_request_delay is not None else (60.0 / self.openai_request_rate_limit_per_min)
        self.openai_model = openai_model if openai_model is not None else "gpt-3.5-turbo"
        self.client = OpenAI(api_key=self.openai_key)
        print("OpenAI client created!")
        print("OpenAI config loaded!")
        print("Initialization Complete!")
    
    @staticmethod
    def preprocess_email_body(body):
        # Remove headers, footers, and signatures using regular expressions
        # This is a basic example, and you may need to customize it based on the structure of your emails
        body = re.sub(r'[\r\n]+', '\n', body)  # Normalize line endings

        # Remove subject lines
        body = re.sub(r'(?i)subject:[^\n]*', '', body)

        body = re.sub(r'(?i)\bfrom\b[^\n]*', '', body)  # Remove lines starting with "from" (common in headers)
        body = re.sub(r'(?i)\bSent\b[^\n]*', '', body)  # Remove lines starting with "Sent" (common in forwarded emails)

        # Remove lines that often indicate the start of a signature or footer
        body = re.sub(r'(?i)\b(?:best|kind|regards|sincerely|thanks|cheers|yours|cordially|thank you)\b[^\n]*', '', body)

        # Remove email addresses
    #     body = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '', body)

        # Remove URLs
        body = re.sub(r'https?://\S+', '', body)

        # Remove tabs and newlines
        body = body.replace('\t', '').replace('\n', '')

        # Add more patterns as needed to remove specific headers, footers, or signatures

        return body.strip()
    
    @staticmethod    
    def remove_html_tags(text):
        soup = BeautifulSoup(text, 'html.parser')
        return soup.get_text()

    @staticmethod
    def remove_contacts(text):
        # Remove mobile numbers
        text_no_numbers = re.sub(r'\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b', '', text)    
        return text_no_numbers
              
              
    @staticmethod          
    def fetch_sender_email_outlook(email):
        try:
            sender_email_property = email.PropertyAccessor.GetProperty("http://schemas.microsoft.com/mapi/proptag/0x5D01001F")
            sender_email = sender_email_property if sender_email_property else None
        except Exception as e:
            print(f"Error accessing MAPIProperties for SenderEmail: {str(e)}")
            sender_email = None

        if not sender_email:
            from_property = email.From
            sender_email = from_property.split(";")[0].strip() if from_property else None

        return sender_email
    
    @staticmethod
    def calculate_response_time(group):
        # Create a new column "Foo" with SentDateTime values for each row
        group['Foo'] = group['SentDateTime']

        # Iterate through each row
        for index, row in group.iterrows():
            # Search for previous rows where sender and receiver are swapped
            mask = (group['SenderEmail'] == row['RecipientEmail']) & (group['RecipientEmail'] == row['SenderEmail']) & (group.index < index)

            # If there are matching rows, update "Bar" and calculate "ResponseTime"
            if any(mask):
                group.at[index, 'Bar'] = group.loc[mask, 'SentDateTime'].iloc[-1]
                group.at[index, 'ResponseTime'] = round((group.at[index, 'Foo'] - group.at[index, 'Bar']).total_seconds()/60)

    #     group["ResponseTime"] = group["ResponseTime"].fillna(0).astype(int)
        return group.sort_values(by='SentDateTime')
    
    @staticmethod
    def time_formatting(df):
        df["Formatted_ResponseTime"] = df["ResponseTime"].apply(
        lambda x: (
            f'{int(x // (60 * 24))} days' if x // (60 * 24) >= 1 else
            f'{int(x // 60)} hours' if x // 60 >= 1 else
            f'{int(x)} minutes' if x != 0 else
            f'{int(x * 60)} seconds'
        ) if not pd.isnull(x) else None)
    #     group["ResponseTime_Minutes"] = round((group["ResponseTime"].dt.total_seconds / 60))
    #     group["Formatted_ResponseTime"] = group["ResponseTime"].apply(
    #         lambda x: f'{x.days} days' if x.days != 0 else (
    #             f'{round(x.seconds // 3600)} hours' if x.seconds // 3600 != 0 else (
    #                 f'{round(x.seconds // 60)} minutes' if x.seconds // 60 != 0 else f'{x.seconds} seconds'
    #             )
    #         ) if not pd.isnull(x) else None
    #     )
    
    def get_all_accounts(self):
        outlook = self.outlookObj.GetNamespace("MAPI")
        accounts = outlook.Accounts
        all_accounts = []
        for account in accounts:
            email_address = account.SmtpAddress
            all_accounts.append(email_address)
        return all_accounts

    def get_client_id_current_profile_df(self):
        client_id_profile_df = pd.read_excel(self.client_id_file_path)
        all_accounts = self.get_all_accounts()
        print(all_accounts)
        client_id_profile_df['Profile Id'] = client_id_profile_df['Profile Id'].str.lower()        
        return client_id_profile_df.loc[client_id_profile_df['Profile Id'].isin(all_accounts)]
    
    def get_emp_dept_mapping(self):
        department_df = pd.read_excel(self.employee_dept_mapping_file_path)
        department_df['Official Email ID'] = department_df['Official Email ID'].str.lower()
        email_department_map = dict(zip(department_df['Official Email ID'], department_df['Department']))
        return email_department_map

    def get_internet_message_id(self, message):
        PR_INTERNET_MESSAGE_ID = "http://schemas.microsoft.com/mapi/proptag/0x1035001E"
        try:
            # Get the PropertyAccessor
            prop_accessor = message.PropertyAccessor
            # Get the property value by property tag
            prop_value = prop_accessor.GetProperty(PR_INTERNET_MESSAGE_ID)
            return prop_value
        except Exception as e:
            print(f"Error getting property value: {e}")
            return None

    def fetch_restricted_emails_outlook(self, user_email, sender_email):
#         inbox = self.outlookObj.GetNamespace("MAPI").GetDefaultFolder(6)  # 6 corresponds to the Inbox folder
        profile = namespace.Folders.Item(user_email)
        # Access the Inbox folder of the specified profile
        inbox = profile.Folders("Inbox")
        messages = inbox.Items
        messages.Sort('[ReceivedTime]')
        # Construct the Restriction query
        restriction_query = f"[SenderEmailAddress]='{sender_email}'"
        
        # Restrict emails based on the query
        filtered_items = messages.Restrict(restriction_query)
        return list(i for i in filtered_items if i.Class == 43)    

    def fetch_conversation_id_messages_from_graph(self, internetMessageId, user_id):
        endpoint_url = f'https://graph.microsoft.com/v1.0/users/{user_id}/mailFolders/inbox/messages?$filter=internetMessageId eq \'{quote(internetMessageId)}\'&$select=conversationId'
        headers = {
            'Authorization': f'Bearer {self.graph_access_token}',
            'Content-Type': 'application/json',
        }

        response = requests.get(endpoint_url, headers=headers)

        try:
            response.raise_for_status()
            data = response.json()
            return data.get('value')
        except requests.exceptions.RequestException as e:
            print(f"Error fetching threads from Graph API: {e}")
            return []

    def fetch_threads_from_graph(self, conversationId, user_id):
        filter_param = f"conversationId eq \'{quote(conversationId)}\'"
        select_param = 'subject,conversationId,receivedDateTime,sentDateTime,sender,emailToRecipients'

        inbox_endpoint_url = f'https://graph.microsoft.com/v1.0/users/{user_id}/mailFolders/inbox/messages?$filter=conversationId eq \'{quote(conversationId)}\''
        sent_items_endpoint_url = f'https://graph.microsoft.com/v1.0/users/{user_id}/mailFolders/SentItems/messages?$filter=conversationId eq \'{quote(conversationId)}\''

        headers = {
            'Authorization': f'Bearer {self.graph_access_token}',
            'Content-Type': 'application/json',
        }

        response_1 = requests.get(inbox_endpoint_url, headers=headers)
        response_2 = requests.get(sent_items_endpoint_url, headers=headers)

        try:
            response_1.raise_for_status()
            response_2.raise_for_status()
            data_1 = response_1.json().get('value')
            data_2 = response_2.json().get('value')
            data_3 = data_1 + data_2
            return data_3
        except requests.exceptions.RequestException as e:
            print(f"Error fetching threads from Graph API: {e}")
            return []
        
    def prepare_dataframe(self, sender_email_to_filter, user_email, client_name):
        #Fetch internetMessageIds from restricted emails
        print(f"Fetching restricted emails for client email address: {sender_email_to_filter}...")
        restricted_emails = self.fetch_restricted_emails_outlook(user_email, sender_email_to_filter)
        print(f"Done!Total no. of emails: {len(restricted_emails)}.")
        if len(restricted_emails) == 0:
            print(f"No emails found for client {sender_email_to_filter} and profile {user_email}")
            return None
        internetMessageIds = set()
        conversationIds = set()
        data_list = []
        threads = []

        print(f"Fetching internetMessageId for each email...")
        for email in restricted_emails:
            sender_email_outlook = EmailMetrics.fetch_sender_email_outlook(email)
            if sender_email_to_filter == sender_email_outlook.lower():
                internetMessageId = self.get_internet_message_id(email)
                internetMessageIds.add(internetMessageId)

        internetMessageIds = list(internetMessageIds)
        print(f"Done! Length of internetMessageIds list: {len(internetMessageIds)}.")

        print(f"Fetching conversationId for each internetmessageId...")
        for internetmessageId in internetMessageIds:
            messages = self.fetch_conversation_id_messages_from_graph(internetmessageId, user_email)
            for message in messages:
                conversationIds.add(message['conversationId'])        
        print(f"Done! Length of unique conversationIds set: {len(conversationIds)}.")        
        print(f"Fetching email thread for each conversationId...")
        for conversationId in conversationIds:
            thread = self.fetch_threads_from_graph(conversationId, user_email)            
            if sender_email_to_filter in thread[0]['from']['emailAddress']['address'].lower():
                print(f"Emails found where the first sender is {sender_email_to_filter}")
                threads.append(thread)
            else:
                print(f"No email threads found with first sender as {sender_email_to_filter}")
                threads.append(thread)
        print(f"Done! Length of threads list: {len(threads)}.")
        print(f"Creating initial dataframe with each email thread conversation...")
        if len(threads) == 0:
            return
        for thread in threads:        
            for email in thread:
                conversation_data = {
                'ConversationId': email.get('conversationId'),
                'Subject': email.get('subject'),
                'SentDateTime': email.get('sentDateTime'),
                'SenderEmail': email.get('sender', {}).get('emailAddress', {}).get('address', '').lower(),
                'ToRecipients': [recipient.get('emailAddress', {}).get('address', '').lower() for recipient in email.get('toRecipients', [])],
                'Body': email.get('body', {}).get('content', '')}
                data_list.append(conversation_data)            

        df_conversations = pd.DataFrame(data_list)
        df_conversations_exploded = df_conversations.explode('ToRecipients')
        df_conversations = df_conversations_exploded.reset_index(drop=True)
        df_conversations.rename(columns={'ToRecipients':'RecipientEmail'}, inplace=True)
        df_conversations["SentDateTime"] = pd.to_datetime(df_conversations["SentDateTime"])
        df_conversations.insert(0, "ProfileName", user_email)
        df_conversations.insert(1, "ClientName", client_name)
        print(f"Done!Dataframe shape: {df_conversations.shape}.") 

        print(f"Inserting Department column...") 
        #Insert Department
        email_department_map = self.get_emp_dept_mapping()

        df_conversations['Department'] = df_conversations['SenderEmail'].map(email_department_map)
        df_conversations.loc[df_conversations["SenderEmail"] == sender_email_to_filter, "Department"] = "Customer"
        df_conversations.loc[df_conversations["SenderEmail"] == "attendance@tascoutsourcing.com", "Department"] = "Fin Ops"
        df_conversations["SenderEmailDomain"] = df_conversations.SenderEmail.apply(lambda x:x.split('@')[1])
        df_conversations.loc[df_conversations["SenderEmailDomain"] != "tascoutsourcing.com", "Department"] = "Customer"
        df_conversations.drop(columns=["SenderEmailDomain"], inplace=True)
        print("Done!")
        print(f"Initial dataframe columns: {df_conversations.columns.tolist()}.")

        print(f"Cleaning up Body content...")
        #Cleanup Body
        df_conversations["Body"] = df_conversations["Body"].apply(EmailMetrics.remove_html_tags)
        df_conversations["Body"] = df_conversations["Body"].apply(EmailMetrics.remove_contacts)
        df_conversations["Body"] = df_conversations["Body"].apply(EmailMetrics.preprocess_email_body)
        print("Done!")        

        print(f"Calculating response time for each conversation belonging to a conversationId...")
        #get response time for each conversationID
        grouped_df = df_conversations.groupby('ConversationId')

        # Define a function to sort each group by 'datetime_column'
        def sort_group(group):
            return group.sort_values(by='SentDateTime')

        # Apply the function to each group
        sorted_grouped_df = grouped_df.apply(sort_group)
        sorted_grouped_df = sorted_grouped_df.reset_index(drop=True)
        grouped_df = sorted_grouped_df.groupby('ConversationId')
        sorted_grouped_df1 = grouped_df.apply(EmailMetrics.calculate_response_time)
        EmailMetrics.time_formatting(sorted_grouped_df1)
        sorted_grouped_df1 = sorted_grouped_df1.reset_index(drop=True)
        sorted_grouped_df1['AssignedTo'] = sorted_grouped_df1['RecipientEmail'].map(email_department_map)
        sorted_grouped_df1.loc[sorted_grouped_df1["AssignedTo"].isnull(), "AssignedTo"] = None
        sorted_grouped_df1.drop(['Foo', 'Bar'], axis=1, inplace=True)
        print("Done!")

        print(f"Dataframe Shape: {sorted_grouped_df1.shape}")
        print(f"Dataframe Columns: {sorted_grouped_df1.columns.tolist()}")
        print("Top 5 rows: ")
        print("=================")
        for index, row in sorted_grouped_df1.head().iterrows():
            print(row)      
        print("Done!")
        return sorted_grouped_df1
    
    def open_ai_query_engine(self, prompt):                      
        # Sleep for the delay
        time.sleep(self.openai_request_delay)
        response = self.client.chat.completions.create(
            model=self.openai_model,
            messages=[{"role": "user", "content": prompt}])

        if(response):            
            print(f'{response.usage.prompt_tokens} prompt tokens counted by the OpenAI API.')           
            return response.choices[0].message.content        
        
    @staticmethod    
    def preprocess_cust_happiness_body(group):  
        
        second_occurrence_indices = (group['Department'] == "Customer").duplicated(keep='first') & (group['Department'] == "Customer").duplicated(keep=False)       
        chb = np.where(second_occurrence_indices, group.Body, None)
        group['Cust_happiness_body'] = chb
        group.loc[group['Department'] != "Customer", 'Cust_happiness_body'] = None
        return group
    
    @staticmethod        
    def post_process_sentiment_priority(df):                
        df["StandardizedSentiment"] = df.Sentiment.str.lower().apply(lambda x: re.search(r'\b(?:negative|positive|neutral)\b', x, flags=re.IGNORECASE).group() if len(re.findall(r'\b(?:negative|positive|neutral)\b', x, flags=re.IGNORECASE)) == 1 else None).fillna('None')
        df["PriorityStandardized"] = df.Priority.str.extract(r'(\b\d+\b)').fillna('None')

    def db_insert(self, dataframe):
        # Create a SQLAlchemy engine
        engine = create_engine(f'postgresql://{self.db_username}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')

        # Insert DataFrame into PostgreSQL database
        dataframe.to_sql('Outlook_email_reporting', engine, if_exists='append', index=False)

        # Close the database connection
        engine.dispose()    

start_time = time.time()    
print(f"**************Script Start**************\nStart time: {datetime.now().time().strftime('%H:%M:%S')}")    

openai_prompts = {   
    "objective_w1": "Summarize the objective of the text in exactly one word: {text}. If the objective is not clear, then default to 'None'",
    "objective_w3": "Summarize the objective of the text in exactly three words: {text}. If the objective is not clear, then default to 'None'",
    "objective_w10": "Summarize the objective of the text in exactly ten words: {text}. If the objective is not clear, then default to 'None'",
    "sentiment": "Classify the sentiment of the following email exactly as positive, negative or neutral: {text}. Consider the overall tone, language, and emotional content of the email. If the content doesn't contain enough information to determine sentiment or if the content is empty, respond with 'None'.",
    "cust_happiness_index": "Provide a sentiment rating for the following email body on a scale from 1 to 5, where 1 is extremely negative,\
    2 is negative, 3 is neutral, 4 is positive, and 5 is extremely positive.\
    If the email body doesn't contain enough information to provide a rating or if the content is empty, please respond with 'None'.\
    Consider the overall tone, language and emotional content of the email.\
    Please respond with exactly a single value only. The value should be either a rating number between 1 and 5 or 'None'.\
    {text}",
    "priority":"Considering both the email subject line and body,\
    assign a priority level to the following text based on urgency, using the following scale:\
    1 - High priority\
    2 - Medium priority\
    3 - Low priority\
    If the content doesn't contain enough information to determine priority, respond with 'None'.\
    Please carefully evaluate the level of urgency, considering factors such as time sensitivity,\
    importance, and any explicit cues in the text.\
    Provide a single value exactly, either a number between 1 and 3 or 'None'.\
    {text}"    
}

dfs = []
no_emails_clients = []
emailMetricsObj = EmailMetrics()
print("Fetching client Id mapping dataframe for current profile...")
client_id_profile_df = emailMetricsObj.get_client_id_current_profile_df()
print(f"Done! Dataframe shape: {client_id_profile_df.shape}")

for index, row in client_id_profile_df.iterrows():
    profile_id = row["Profile Id"].lower()
    client_name = row["Client_Name"]
    client_email = row["Client_email_id"].lower()
    print(f"Initializing dataframe preparation for user_profile: {profile_id}, client_name: {client_name}, client_email: {client_email}")
    df = emailMetricsObj.prepare_dataframe(sender_email_to_filter=client_email,\
                                           user_email=profile_id,\
                                           client_name=client_name)
    if df is None or df.empty:
        print(f"No dataframe will be created for this {client_email}")
        no_emails_clients.append((profile_id, client_email))
        continue
    print("Initiating OpenAI feature enrichment...(1)Objectives")
    #Objectives
    df["Objective_w1"] = df.Body.apply(lambda x: emailMetricsObj.open_ai_query_engine(prompt=openai_prompts["objective_w1"].format(text=x)))
    df["Objective_w3"] = df.Body.apply(lambda x: emailMetricsObj.open_ai_query_engine(prompt=openai_prompts["objective_w3"].format(text=x)))
    df["Objective_w10"] = df.Body.apply(lambda x: emailMetricsObj.open_ai_query_engine(prompt=openai_prompts["objective_w10"].format(text=x)))
    print("Done! OpenAI features for Objectives added to dataframe")
    print(f"Dataframe columns: {df.columns.tolist()}")
    
    print("Initiating OpenAI feature enrichment...(2)Customer Happiness Index")
    #Customer Happiness index
    df = df.groupby('ConversationId').apply(EmailMetrics.preprocess_cust_happiness_body).reset_index(drop=True)    
    df["Customer_Happiness_Index"] = df["Cust_happiness_body"].apply(lambda x: emailMetricsObj.open_ai_query_engine(prompt=openai_prompts["cust_happiness_index"].format(text=x)))
    df.drop(columns = ["Cust_happiness_body"], inplace=True)
    print("Done! OpenAI feature for Customer Happiness Index added to dataframe")
    print(f"Dataframe columns: {df.columns.tolist()}")
    
    print("Initiating OpenAI feature enrichment...(3)Sentiment")
    #Sentiment & priority
    df["Sentiment"] = df.Body.apply(lambda x: emailMetricsObj.open_ai_query_engine(prompt=openai_prompts["sentiment"].format(text=x)))
    print("Done! OpenAI feature for Sentiment added to dataframe")
    
    print("Initiating OpenAI feature enrichment...(4)Priority")
    df["Priority"] = df.Body.apply(lambda x: emailMetricsObj.open_ai_query_engine(prompt=openai_prompts["priority"].format(text=x)))    
    print("Done! OpenAI feature for Sentiment added to dataframe")
    print("Post-processing Sentiment and Priority columns...")
    EmailMetrics.post_process_sentiment_priority(df)
    print("Done!")
    print(f"Dataframe columns: {df.columns.tolist()}")
    dfs.append(df)
    print("Initiating dataframe insertion into database table 'Outlook_email_reporting'...")
    emailMetricsObj.db_insert(df)
    print(f"Done! Dataframe of shape {df.shape} inserted into database successfully!")
    time.sleep(60)

end_time = time.time()     
print(f"**************Script End**************\nEnd time: {datetime.now().time().strftime('%H:%M:%S')}")
print(f"Time taken for script execution: {end_time-start_time} seconds")
