import datetime

IGNOREDDIRECTORIES = [".venv", ".git", "__pycache__", "backend", "frontend"]

OPENAIRELEASES = {"GPT-1": datetime.date(2018,6,11),            #https://github.com/jqueryscript/chatgpt-timeline
                  "GPT-2 public": datetime.date(2019,11,5),      #https://github.com/jqueryscript/chatgpt-timeline
                  "GPT-3": datetime.date(2020,6,11),            #https://github.com/jqueryscript/chatgpt-timeline
                  "GPT-3.5": datetime.date(2022,11,30),         #https://en.wikipedia.org/wiki/GPT-3#GPT-3.5
                  "GPT-4": datetime.date(2023,3,14),            #https://en.wikipedia.org/wiki/GPT-4
                  "GPT-4o": datetime.date(2024,5,13),           #https://en.wikipedia.org/wiki/GPT-4o
                  "GPT o1": datetime.date(2024,12,5),           #https://en.wikipedia.org/wiki/OpenAI_o1
                  "GPT-4.5": datetime.date(2025,2,27),          #https://en.wikipedia.org/wiki/GPT-4.5
                  "GPT-4.1": datetime.date(2025,4,14),          #https://en.wikipedia.org/wiki/GPT-4.1
                  "GPT o3": datetime.date(2025,4,16),           #https://en.wikipedia.org/wiki/OpenAI_o3
                  "GPT-5": datetime.date(2025,8,7),             #https://en.wikipedia.org/wiki/GPT-5
                  "GPT-5.1": datetime.date(2025,11,12),         #https://en.wikipedia.org/wiki/GPT-5.1
                  "GPT-5.2": datetime.date(2025,12,11),         #https://en.wikipedia.org/wiki/GPT-5.2
                  "GPT-5.4": datetime.date(2026,3,5),}          #https://en.wikipedia.org/wiki/GPT-5.4


#Only major releases are here because it would clutter everything otherwise
GOOGLERELEASES = {"Google Bard": datetime.date(2023,3,21),          #https://en.wikipedia.org/wiki/Gemini_(language_model)#Model_versions
                  "Gemini 1.0 Pro": datetime.date(2024,5,13),       #https://en.wikipedia.org/wiki/Gemini_(language_model)#Model_versions      
                  "Gemini 1.5 Pro": datetime.date(2025,2,27),       #https://en.wikipedia.org/wiki/Gemini_(language_model)#Model_versions
                  "Gemini 2.0 Pro": datetime.date(2025,4,16),       #https://en.wikipedia.org/wiki/Gemini_(language_model)#Model_versions
                  "Gemini 2.5 Pro": datetime.date(2025,8,7),        #https://en.wikipedia.org/wiki/Gemini_(language_model)#Model_versions
                  "Gemini 3 Pro": datetime.date(2025,11,12),        #https://en.wikipedia.org/wiki/Gemini_(language_model)#Model_versions
                  "Gemini GPT-5.2": datetime.date(2025,12,11),      #https://en.wikipedia.org/wiki/Gemini_(language_model)#Model_versions
                  "Gemini GPT-5.4": datetime.date(2026,3,5),}       #https://en.wikipedia.org/wiki/Gemini_(language_model)#Model_versions


OTHER = {"Covid pandemic": datetime.date(2020,3,1),                #https://www.nm.org/healthbeat/medical-advances/new-therapies-and-drug-trials/covid-19-pandemic-timeline#:~:text=COVID%2D19%20Pandemic%20Timeline,what%20happened%20along%20the%20way.
         "Stack Overflow acquisition": datetime.date(2021,6,2)     #https://en.wikipedia.org/wiki/Stack_Exchange
         }                

IMPORTANTDATES = OPENAIRELEASES | GOOGLERELEASES | OTHER