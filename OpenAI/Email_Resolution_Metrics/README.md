A script to compute email SLA metrics for a typical customer support helpdesk. The following metrics are determined for each email thread conversation between a customer and support personnel from various departments:
- Customer Happiness Index
- Response Time (for each email in a thread) of the investigating support team
- Email priority
- Email sentiment
- Email objective in one word, three words and ten words

Except Response Time, all other metrics were computed using OpenAI prompts. The `openai` Python library was used.
For fetching email data, the Microsoft's Graph API and COM API was used.

The script manages OpenAI API's `RateLimitError` by adding a delay before every subsequent API call. The delay is computed based on the Rate limits set by the OpenAI Tier subscription.
To manage the API's `TokenLimitError`, the email body is pre-processed before sending to the API.

All computations are managed with a pandas dataframe and then imported into a SQL database.

___Script specs___:

- OpenAI Model: __GPT-3.5-turbo__
- Database: __PostGreSQL__
- APIs: __MS Graph API__, __Open AI__, __COM Object__
- Major Python Libraries: __openai__, __pywin32__, __requests__, __pandas__
