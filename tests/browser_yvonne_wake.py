"""Browser regression check with simulated local recognition.
Run tests/preview_yvonne_wake.py via uvicorn on localhost:8771 first.
Requires Playwright and Chromium; makes no real OpenAI requests.
"""
from playwright.sync_api import sync_playwright,expect
expect.set_options(timeout=15000)
FAKE=r'''window.localCalls=[];window.wakeStarts=0;window.wakeAborts=0;window.remoteAttempts=0;
class LocalRecognition {
 constructor(){this.processLocally=false;window.lastWake=this;}
 static async available(o){window.localCalls.push(o);return o.langs[0]==='nb-NO'?'unavailable':'available';}
 static async install(o){window.localCalls.push(o);return true;}
 start(){if(!this.processLocally)window.remoteAttempts++;window.wakeStarts++;setTimeout(()=>this.onstart?.(),0);}
 abort(){window.wakeAborts++;}
}window.SpeechRecognition=LocalRecognition;
window.sayWake=(text)=>{let r=[[{transcript:text}]];window.lastWake.onresult?.({resultIndex:0,results:r});};
window.level=0;AnalyserNode.prototype.getFloatTimeDomainData=function(d){d.fill(window.level);};'''
with sync_playwright() as p:
 b=p.chromium.launch(executable_path='/usr/bin/chromium',headless=True,args=['--no-sandbox','--use-fake-ui-for-media-stream','--use-fake-device-for-media-stream'])
 c=b.new_context(permissions=['microphone']);c.add_init_script(FAKE)
 errors=[];requests=[]
 a=c.new_page();a.on('pageerror',lambda e:errors.append(str(e)));a.on('request',lambda r:requests.append(r.url) if '/johnny/ask' in r.url else None)
 a.goto('http://localhost:8771/');a.locator('#nameIn').fill('Local test');a.locator('#wantJohnny').check();a.locator('#joinBtn').click();expect(a.locator('#app')).to_be_visible();a.locator('#johnnyYes').click();expect(a.locator('#yvonneWakeControls')).to_be_visible()
 assert not a.locator('#yvonneWake').is_checked();a.locator('#yvonneWake').check();expect(a.locator('#yvonneWakeStatus')).to_contain_text('Lytter lokalt');assert a.evaluate('window.localCalls.every(x=>x.processLocally===true)')
 a.evaluate('sayWake("Vi snakker om været")');a.wait_for_timeout(700);assert not requests;expect(a.locator('#johnnyAsk')).to_have_text('Spør Yvonne')
 a.locator('#micBtn').click();assert a.evaluate('window.lastWake.onresult===null');starts=a.evaluate('wakeStarts');a.wait_for_timeout(600);assert a.evaluate('wakeStarts')==starts;a.locator('#micBtn').click();expect(a.locator('#yvonneWakeStatus')).to_contain_text('Lytter lokalt')
 a.evaluate('window.level=.06;sayWake("Yvonne")');expect(a.locator('#johnnyAsk')).to_have_text('Send spørsmålet');a.wait_for_timeout(800)
 with a.expect_response('**/meet/johnny/ask') as reply:
  a.evaluate('window.level=0')
 assert reply.value.status==200 and len(requests)==1
 expect(a.locator('#chatLog')).to_contain_text('Yvonne · AI');expect(a.locator('#yvonneWakeStatus')).to_contain_text('Lytter lokalt')
 # A silent accidental trigger must not upload anything.
 a.evaluate('sayWake("Yvonne")');expect(a.locator('#johnnyAsk')).to_have_text('Send spørsmålet');a.wait_for_timeout(8500);assert len(requests)==1
 expect(a.locator('#yvonneWakeStatus')).to_contain_text('Lytter lokalt');a.evaluate('window.level=.06;sayWake("Yvonne")');expect(a.locator('#johnnyAsk')).to_have_text('Send spørsmålet')
 # New participant revokes consent and discards current question.
 d=c.new_page();d.goto(a.url);d.locator('#nameIn').fill('New participant');d.locator('#joinBtn').click();expect(d.locator('#app')).to_be_visible();expect(a.locator('#yvonneWakeControls')).not_to_be_visible();assert not a.locator('#yvonneWake').is_checked();assert len(requests)==1
 a.locator('#johnnyYes').click();d.locator('#johnnyYes').click();expect(a.locator('#yvonneWakeControls')).to_be_visible();assert not a.locator('#yvonneWake').is_checked()
 # Unsupported browser cannot start recognition or fall back to cloud.
 a.evaluate('()=>{window.SpeechRecognition=class {static async available(){return "available"}};}');a.locator('#yvonneWake').click();expect(a.locator('#yvonneWakeStatus')).to_contain_text('ikke tilgjengelig');assert not a.locator('#yvonneWake').is_checked();assert a.evaluate('remoteAttempts')==0
 a.locator('#johnnyAsk').click();a.wait_for_timeout(500)
 with a.expect_response('**/meet/johnny/ask') as manual:a.locator('#johnnyAsk').click()
 assert manual.value.status==200 and len(requests)==2
 d.locator('#johnnyStop').click();expect(a.locator('#johnnyAsk')).not_to_be_visible();assert not errors,errors
 a.locator('#leaveBtn').click();d.locator('#leaveBtn').click();b.close()
 print('PASS: local-only recognition, mute, wake question, silence auto-send, empty-trigger discard, consent reset, unsupported browser, manual fallback; no JS errors.')
