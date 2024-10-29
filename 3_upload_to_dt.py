from requests import post
from pathlib import Path
import time

# %% function
def reload_data(files: list[Path], hash_db: str, token: str, wait: int = 15 ):
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

    Raises
    ------
    ValueError
        Response from the server: not successful upload.

    Returns
    -------
    None.

    """
    
    
    # reload first base
    action = "reload"
    url = f"https://dataview.aya-research.ru/api/v2/upload/{action}/{hash_db}"
    
    print(files[0])
    res = post(url,
            headers={"token": token},
            files={'dataset': open(files[0] ,'rb')})
    
    # append other bases
    if not (200 <= res.status_code < 300):
        raise ValueError(f"RESPONSE: {res.status_code}, {res.text}")
    
    elif (400 <= res.status_code < 500):
        time.sleep(wait)
        res = post(url,
                headers={"token": token},
                files={'dataset': open(files[0] ,'rb')})
        
    
    else:
        print(f"RESPONSE: {res.status_code}, {res.text}")
        time.sleep(wait)
        
        for f in files[1:]:
            print(f)
            action = "append"
            url = f"https://dataview.aya-research.ru/api/v2/upload/{action}/{hash_db}"
            res = post(url,
                    headers={"token": token},
                    files={'dataset': open(f ,'rb')})
            
            if 200 <= res.status_code < 300:
                print(f"RESPONSE: {res.status_code}, {res.text}")
                time.sleep(wait)

            
            
            elif (400 <= res.status_code < 500):
                print(ValueError(f"RESPONSE: {res.status_code}, {res.text}"))
                print('will_try_again')
                time.sleep(wait)
                time.sleep(wait)
                print('try_again')
                print(f)
                res = post(url,
                            headers={"token": token},
                            files={'dataset': open(f,'rb')})

                    
                
                
            else:
                raise ValueError(f"RESPONSE: {res.status_code}, {res.text}")

# %% run reload
with open('../token', 'r') as t:
    token = t.read()
    
files = list(Path('exports/').glob('*.zip'))
hash_db = "ce51522a-44d2-476b-a305-68e812555a37"

reload_data(files, hash_db, token )

