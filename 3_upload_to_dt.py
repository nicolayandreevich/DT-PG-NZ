from requests import post
from pathlib import Path
import time

# %% function
def upload_data(files: list[Path], hash_db: str, token: str, wait: int = 15, mode: str = 'reload'):
    """
    Reloads existing database with list of files.
    
    Parameters
    ----------
    files : list
        List of the files to upload.
    hash_db : str
        Hash of the database to reload.
    token : str
        DT token.
    wait : int, optional
        Seconds to wait for data processing on the server. Bigger files require more time. The default is 15.
    mode : str, optional
        Action mode for upload. Put "reload" to replace existing data, put "append" for appending to existing data. The default is "reload".

    Raises
    ------
    ValueError
        Response from the server: not successful upload.

    Returns
    -------
    None.

    """
    
    def post_wait(file, action):
        print(file, action)
        url = f"https://dataview.aya-research.ru/api/v2/upload/{action}/{hash_db}"
        
        res = post(url,
                headers={"token": token},
                files={'dataset': open(file, 'rb')})  
        
        if not (200 <= res.status_code < 300):
            if (400 <= res.status_code < 500):
                print(f"RESPONSE: {res.status_code}, {res.text}")
                print('trying again')
                time.sleep(wait)
                res = post(url,
                        headers={"token": token},
                        files={'dataset': open(file ,'rb')})
                
                if not (200 <= res.status_code < 300):
                    raise ValueError(f"RESPONSE: {res.status_code}, {res.text}")
            else:
                raise ValueError(f"RESPONSE: {res.status_code}, {res.text}")

        print(f"RESPONSE: {res.status_code}, {res.text}")
        time.sleep(wait)
    
    
    if mode == 'reload':
        post_wait(files[0], "reload")
        for f in files[1:]:
            post_wait(f, "append")
            
    elif mode == 'append':
        for f in files:
            post_wait(f, "append")
    
    else:
        raise ValueError(f"{mode} is not supported mode for upload. Must be 'reload' or 'append'")
        

# %% run reload
with open('../token', 'r') as t:
    token = t.read()
hash_db = "ce51522a-44d2-476b-a305-68e812555a37"

#Add old data (replace all data in DT)    
#files = list(Path('exports/').glob('*.zip'))
#upload_data(files, hash_db, token, wait=45, mode='reload')

#Add new data 
files = list(Path('exports/new_data/').glob('*.zip'))
upload_data(files, hash_db, token, wait=45, mode='append')
