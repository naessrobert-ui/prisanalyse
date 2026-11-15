"""Explicit localhost-only demo with generated pictures and disposable test credentials.
Never deploy this test server. No real family data, OpenAI calls or TURN credentials.
"""
import hashlib
import json
import secrets
import sqlite3
import tempfile
import time
from pathlib import Path

from fastapi import HTTPException, Request
from PIL import Image

from familie.server import create_app, code_hash

_folder=tempfile.TemporaryDirectory(prefix='fruen-local-demo-')
root=Path(_folder.name)
salt=secrets.token_hex(16)
(root/'access.json').write_text(json.dumps({'salt':salt,'code_hash':code_hash('family-local-test-only',salt),'session_key':secrets.token_hex(32)}))
salt=secrets.token_bytes(16)
(root/'fruen-admin.json').write_text(json.dumps({'salt':salt.hex(),'hash':hashlib.scrypt(b'fruen-local-test-only',salt=salt,n=16384,r=8,p=1).hex()}))
app=create_app(root,secure_cookie=False)
with sqlite3.connect(root/'posts.sqlite3') as conn:
    for color,title in [('#20685a','Et testbilde fra familien'),('#c67d4d','Et nytt øyeblikk'),('#527293','Gode minner')]:
        ident=secrets.token_hex(16);path=root/'media'/(ident+'.png')
        Image.new('RGB',(900,600),color).save(path)
        conn.execute('INSERT INTO posts(id,mime,suffix,name,caption,created,expires,bytes) VALUES(?,?,?,?,?,?,?,?)',(ident,'image/png','.png','Lokal test',title,time.time(),0,path.stat().st_size))

@app.post('/fruen/api/test/night')
async def local_night(on:int):
    # Demo/test only: force night mode on (1), off (0) or back to the clock (-1).
    import familie.fruen as fruen_module
    fruen_module.night_override={1:True,0:False}.get(on)
    return {'night':fruen_module.night_override}


@app.get('/fruen/api/ice')
async def local_ice(request:Request):
    token=request.cookies.get('motes_fruen_session','')
    with sqlite3.connect(root/'posts.sqlite3') as conn:
        row=conn.execute('SELECT 1 FROM fruen_accounts WHERE token=? AND active=1 AND expires>?',(hashlib.sha256(token.encode()).hexdigest(),time.time())).fetchone()
    if not row:raise HTTPException(401,'Inviter denne testenheten først.')
    return {'iceServers':[],'turn':False}
