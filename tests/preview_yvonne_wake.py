"""Explicit local test server only; fake AI providers, no real API calls."""
import asyncio,math,struct
from fastapi import FastAPI
import meet_routes
async def answer(audio,mime):
    return {'question':'Et testspørsmål?', 'text':'Et varmt svar med kilde.', 'citations':[{'start':19,'end':24,'url':'https://www.ssb.no/','title':'SSB'}]}
async def voice(text,on_audio):
    pcm=struct.pack('<'+'h'*2400,*[int(math.sin(i*2*math.pi*440/24000)*3000) for i in range(2400)])
    for _ in range(8):
        await on_audio(pcm)
        await asyncio.sleep(.1)
    return {'model':'gpt-live-1','voice':'marin','finalized':True}
meet_routes.johnny_ai.answer=answer
meet_routes.yvonne_voice.speak=voice
app=FastAPI();app.include_router(meet_routes.router)
@app.get('/')
async def root():return await meet_routes.meet_side()
