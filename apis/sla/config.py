import os

BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join(BASE_DIR, "clearline_sla_template.docx")

MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

ORDINAL = {
    1:"1st",  2:"2nd",  3:"3rd",  4:"4th",  5:"5th",  6:"6th",
    7:"7th",  8:"8th",  9:"9th",  10:"10th",11:"11th",12:"12th",
    13:"13th",14:"14th",15:"15th",16:"16th",17:"17th",18:"18th",
    19:"19th",20:"20th",21:"21st",22:"22nd",23:"23rd",24:"24th",
    25:"25th",26:"26th",27:"27th",28:"28th",29:"29th",30:"30th",
    31:"31st",
}

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://zxxkcvkrpdsvfljrjgqy.supabase.co")
SUPABASE_KEY = os.getenv(
    "SUPABASE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inp4eGtjdmtycGRzdmZsanJqZ3F5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3Mjc0NjM4MiwiZXhwIjoyMDg4MzIyMzgyfQ.nBagWuf_0VhJB5O9CRIBNQyGLLwo236uvjcHei5cG1U",
)

# Dropbox Sign
DROPBOX_SIGN_API_KEY  = os.getenv(
    "DROPBOX_SIGN_API_KEY",
    "6359a5d88bfafbfeb50793fdaf2d58d26872d84d53717364ba52f2ca90209a66",
)
DROPBOX_SIGN_TEST_MODE = os.getenv("DROPBOX_SIGN_TEST_MODE", "true").lower() == "true"

# Default signers
DIRECTOR_NAME     = os.getenv("DIRECTOR_NAME",     "Director")
DIRECTOR_EMAIL    = os.getenv("DIRECTOR_EMAIL",    "leocasey0@gmail.com")
LEGAL_HEAD_NAME   = os.getenv("LEGAL_HEAD_NAME",   "Head, Legal Services")
LEGAL_HEAD_EMAIL  = os.getenv("LEGAL_HEAD_EMAIL",  "leocasey0@gmail.com")
HR_EMAIL          = os.getenv("HR_EMAIL",          "leocasey0@gmail.com")
