"""Local-only administrator initialization. Never accepts a password as a CLI argument."""
import argparse
import getpass
import hashlib
import json
import os
import secrets
import sqlite3
from pathlib import Path


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--reset-admin',action='store_true');args=parser.parse_args()
    root=Path(os.environ.get('FAMILY_DATA_DIR',Path.home()/'motes-familie/data'))
    if not (root/'access.json').is_file():raise SystemExit('Fant ikke familierommet. Stoppet uten endringer.')
    target=root/'fruen-admin.json'
    if target.exists() and not args.reset_admin:
        print('Besøksadministrator er allerede satt opp. Eksisterende kode og enheter er beholdt.');return
    code=getpass.getpass('Velg administratorkode for Fruen (12–200 tegn): ')
    repeated=getpass.getpass('Gjenta administratorkoden: ')
    if code!=repeated or not 12<=len(code)<=200:raise SystemExit('Kodene må være like og ha 12–200 tegn. Ingen kode endret.')
    salt=secrets.token_bytes(16)
    value={'salt':salt.hex(),'hash':hashlib.scrypt(code.encode(),salt=salt,n=16384,r=8,p=1).hex()}
    temp=root/('.fruen-admin-'+secrets.token_hex(8))
    try:
        fd=os.open(temp,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'w') as stream:json.dump(value,stream);stream.flush();os.fsync(stream.fileno())
        os.replace(temp,target)
    finally:temp.unlink(missing_ok=True)
    if args.reset_admin and (root/'posts.sqlite3').is_file():
        with sqlite3.connect(root/'posts.sqlite3') as conn:
            if conn.execute("SELECT 1 FROM sqlite_master WHERE name='fruen_admin_sessions'").fetchone():conn.execute('DELETE FROM fruen_admin_sessions')
    print('Administrator er satt opp. Åpne https://møtes.no/fruen/admin og inviter godkjente familiemedlemmer og bildeskjermen.')

if __name__=='__main__':main()
