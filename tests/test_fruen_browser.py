"""End-to-end visits in real Chrome with fake cameras, against the localhost-only demo.

Separate browser contexts act as separate devices (own cookies). Run explicitly:
    python -m pytest -q tests/test_fruen_browser.py
Skipped when Playwright or Chrome is unavailable. Never uses real family data.
"""
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

playwright_api = pytest.importorskip('playwright.sync_api')
CHROME = os.environ.get('FRUEN_CHROME') or shutil.which('google-chrome') or shutil.which('chromium')
pytestmark = pytest.mark.skipif(not CHROME, reason='Chrome er ikke installert')

ADMIN_CODE = 'fruen-local-test-only'
HEAD = {'X-Fruen-Request': '1'}
# Test hooks only: record peer connections and every captured stream, optionally delay
# or deny camera access. The application code itself is unchanged.
INIT = """
window.__pcs=[];window.__streams=[];window.__fs=0;
const nativeFs=Element.prototype.requestFullscreen;Element.prototype.requestFullscreen=function(...a){window.__fs++;return nativeFs.apply(this,a);};
window.__gumDelay=0;window.__gumDeny=false;
const NativePC=window.RTCPeerConnection;
window.RTCPeerConnection=function(...a){const pc=new NativePC(...a);window.__pcs.push(pc);return pc;};
window.RTCPeerConnection.prototype=NativePC.prototype;
const nativeGum=navigator.mediaDevices.getUserMedia.bind(navigator.mediaDevices);
navigator.mediaDevices.getUserMedia=async c=>{
  if(window.__gumDeny)throw new DOMException('denied','NotAllowedError');
  if(window.__gumDelay)await new Promise(r=>setTimeout(r,window.__gumDelay));
  const s=await nativeGum(c);
  if(window.__forceMuted>0){window.__forceMuted--;s.getTracks().forEach(t=>Object.defineProperty(t,'muted',{configurable:true,get:()=>true}));}
  window.__streams.push(s);return s;};
window.__forceMuted=0;
window.__muteLive=()=>window.__streams.at(-1).getTracks().forEach(t=>Object.defineProperty(t,'muted',{configurable:true,get:()=>true}));
window.__live=()=>window.__streams.flatMap(s=>s.getTracks()).filter(t=>t.readyState!=='ended').length;
window.__inbound=async()=>{const pc=window.__pcs.at(-1);const r={audio:0,video:0};if(!pc)return r;
  (await pc.getStats()).forEach(s=>{if(s.type==='inbound-rtp')r[s.kind]=s.bytesReceived||0;});return r;};
"""


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


@pytest.fixture(scope='module')
def base():
    repo = Path(__file__).resolve().parents[1]
    port = free_port()
    proc = subprocess.Popen([sys.executable, '-m', 'uvicorn', 'preview_fruen:app', '--app-dir', 'tests',
                             '--host', '127.0.0.1', '--port', str(port), '--log-level', 'warning'],
                            cwd=repo, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    url = f'http://localhost:{port}'
    for _ in range(100):
        try:
            if httpx.get(url + '/fruen/api/health', timeout=1, trust_env=False).status_code == 200:
                break
        except httpx.HTTPError:
            time.sleep(.1)
    else:
        proc.kill()
        pytest.fail('Demoserveren startet ikke: ' + proc.stdout.read().decode(errors='replace'))
    yield url
    proc.terminate()
    proc.wait(10)


@pytest.fixture(scope='module')
def browser():
    with playwright_api.sync_playwright() as p:
        b = p.chromium.launch(executable_path=CHROME, args=[
            '--use-fake-device-for-media-stream', '--use-fake-ui-for-media-stream', '--no-sandbox'])
        yield b
        b.close()


@pytest.fixture
def admin(browser, base):
    ctx = browser.new_context(base_url=base)
    assert ctx.request.post('/fruen/api/admin/login', headers=HEAD, data={'code': ADMIN_CODE}).ok
    ctx.request.post('/fruen/api/test/night?on=0')  # tests must not depend on the clock
    created = []

    def invite(name, kind='member'):
        r = ctx.request.post('/fruen/api/admin/invite', headers=HEAD, data={'name': name, 'kind': kind})
        assert r.ok, r.text()
        return r.json()['token']

    def device(name, kind='member', activate=True, viewport=None):
        token = invite(name, kind)
        dctx = browser.new_context(base_url=base, permissions=['camera', 'microphone'], bypass_csp=True,
                                   viewport=viewport or {'width': 1180, 'height': 820})
        dctx.add_init_script(INIT)
        page = dctx.new_page()
        page.goto('/fruen#invite=' + token)
        page.click('#enrol')
        page.wait_for_selector('#activate', state='visible')
        if activate:
            page.click('#activate')
            page.wait_for_selector('#setup', state='hidden')
        created.append((name, dctx))
        return page

    invite.device = device
    invite.created = created
    invite.ctx = ctx
    yield invite
    accounts = ctx.request.get('/fruen/api/admin/accounts').json()['accounts']
    names = {name for name, _ in created}
    for a in accounts:
        if a['active'] and a['name'] in names:
            ctx.request.post('/fruen/api/admin/revoke/' + a['id'], headers=HEAD, data={})
    for _, dctx in created:
        dctx.close()
    ctx.close()


def live(page):
    return page.evaluate('window.__live()')


def wait_online(page, name, timeout=15000):
    page.click('#openContacts')
    page.wait_for_selector(f'#contacts button:has-text("{name}"):not([disabled])', timeout=timeout)


def call(caller, name):
    wait_online(caller, name)
    caller.click(f'#contacts button:has-text("{name}")')
    caller.wait_for_selector('#ringing', state='visible')


def assert_media_flows(*pages):
    for page in pages:
        page.wait_for_selector('#visit', state='visible', timeout=20000)
        page.wait_for_function('document.getElementById("remote").videoWidth>0', timeout=20000)
    first = [p.evaluate('window.__inbound()') for p in pages]
    time.sleep(2)
    second = [p.evaluate('window.__inbound()') for p in pages]
    for a, b in zip(first, second):
        assert b['audio'] > a['audio'] > 0, (a, b)
        assert b['video'] > a['video'] > 0, (a, b)


def assert_back_to_photos(page):
    page.wait_for_selector('#visit', state='hidden', timeout=10000)
    page.wait_for_function('window.__live()===0', timeout=5000)
    page.wait_for_function('window.__pcs.every(pc=>pc.connectionState==="closed")', timeout=5000)
    page.wait_for_selector('#photo[src]:not([hidden])', timeout=15000)


def test_manual_visits_both_directions(admin):
    a = admin.device('Anne')
    b = admin.device('Bjørn')
    assert live(a) == 0 and live(b) == 0  # activation sample is stopped again
    call(a, 'Bjørn')
    b.wait_for_selector('#answer', state='visible')
    assert live(a) == 0 and live(b) == 0  # no camera while ringing
    b.click('#answer')
    assert_media_flows(a, b)
    a.click('#hangup')
    assert_back_to_photos(a)
    assert_back_to_photos(b)
    call(b, 'Anne')
    a.click('#answer')
    assert_media_flows(a, b)
    b.click('#hangup')
    assert_back_to_photos(a)
    assert_back_to_photos(b)
    assert all(len(p.evaluate('window.__pcs')) == 2 for p in (a, b))  # exactly one connection per visit


def test_automatic_answer_after_notice_without_controls(admin):
    frame = admin.device('Fruen', 'auto')
    a = admin.device('Anne')
    assert frame.is_hidden('#controls') and frame.is_hidden('#openContacts')
    assert frame.evaluate('window.__fs') >= 1 and a.evaluate('window.__fs') == 0  # only her frame
    frame.wait_for_selector('#photo[src]:not([hidden])')
    call(a, 'Fruen')
    frame.wait_for_selector('#ringName:has-text("Anne kommer på besøk")')
    assert frame.is_hidden('#answer') and frame.is_hidden('#decline')
    started = time.time()
    assert_media_flows(a, frame)
    assert time.time() - started >= 3  # notice is shown before the camera starts
    assert frame.is_hidden('#controls') and frame.is_hidden('#local')
    a.click('#hangup')
    assert_back_to_photos(frame)
    assert_back_to_photos(a)


def test_decline_cancel_and_late_camera_are_released(admin):
    a = admin.device('Anne')
    b = admin.device('Bjørn')
    frame = admin.device('Fruen', 'auto')
    call(a, 'Bjørn')
    b.wait_for_selector('#decline', state='visible')
    b.click('#decline')
    a.wait_for_selector('#ringing', state='hidden', timeout=10000)
    assert live(a) == 0 and live(b) == 0
    # Camera permission resolves only after the caller has already hung up.
    before = frame.evaluate('window.__streams.length')
    frame.evaluate('window.__gumDelay=6000')
    call(a, 'Fruen')
    frame.wait_for_function(f'window.__streams.length=={before}', timeout=1000)
    time.sleep(8)  # auto-answer has started waiting for the camera
    a.click('#hangup')
    frame.wait_for_selector('#ringing', state='hidden', timeout=10000)
    time.sleep(6)
    assert frame.evaluate('window.__streams.length') <= before + 1
    assert live(frame) == 0 and live(a) == 0
    assert frame.is_hidden('#visit')


def test_frame_ready_even_when_ios_keeps_audio_paused(admin, browser, base):
    # Simulates iPad Safari leaving Web Audio suspended after the camera check.
    token = admin('Fruen', 'auto')
    ctx = browser.new_context(base_url=base, permissions=['camera', 'microphone'], bypass_csp=True)
    ctx.add_init_script(INIT + """
      Object.defineProperty(AudioContext.prototype,'state',{get(){return 'suspended';}});
      AudioContext.prototype.resume=function(){return new Promise(()=>{});};""")
    admin.created.append(('Fruen', ctx))
    frame = ctx.new_page()
    frame.goto('/fruen#invite=' + token)
    frame.click('#enrol')
    frame.click('#activate')
    frame.wait_for_selector('#setup', state='hidden', timeout=10000)
    a = admin.device('Anne')
    call(a, 'Fruen')
    frame.wait_for_selector('#ringName:has-text("Anne kommer på besøk")')
    assert_media_flows(a, frame)
    a.click('#hangup')
    assert_back_to_photos(frame)


def test_caller_is_told_why_the_visit_ended(admin):
    frame = admin.device('Fruen', 'auto')
    a = admin.device('Anne')
    b = admin.device('Bjørn')
    frame.evaluate('window.__gumDeny=true')  # e.g. Safari camera permission not set to Allow
    call(a, 'Fruen')
    a.wait_for_selector('#state:has-text("Fruen sin enhet fikk ikke startet kamera")', timeout=20000)
    assert 'Tillat' in a.text_content('#state') and a.is_hidden('#visit')
    call(b, 'Anne')
    a.wait_for_selector('#decline', state='visible')
    a.click('#decline')
    b.wait_for_selector('#state:has-text("Anne kan ikke ta imot besøk")', timeout=10000)


def test_caller_is_told_when_picture_is_black_or_missing(admin):
    frame = admin.device('Fruen', 'auto')
    a = admin.device('Anne')
    call(a, 'Fruen')
    assert_media_flows(a, frame)
    time.sleep(3)
    assert a.is_hidden('#state')  # normal picture and sound: no message
    frame.evaluate('window.__streams.at(-1).getVideoTracks()[0].enabled=false')  # camera covered: black frames
    a.wait_for_selector('#state:has-text("helt svart eller svært mørkt")', timeout=15000)
    frame.evaluate('window.__streams.at(-1).getVideoTracks()[0].enabled=true')
    a.wait_for_function('document.getElementById("state").hidden', timeout=15000)  # picture back: message cleared
    frame.evaluate('window.__pcs.at(-1).getSenders().forEach(s=>s.replaceTrack(null))')  # nothing sent, tracks still look fine
    a.wait_for_selector('#state:has-text("Ingen bilde fra Fruen")', timeout=15000)
    assert 'Ingen lyd fra Fruen' in a.text_content('#state')
    assert frame.is_hidden('#state') or 'Ingen' not in frame.text_content('#state')  # the frame itself shows nothing
    a.click('#hangup')


def test_frame_restarts_camera_that_ios_stopped_mid_visit(admin):
    frame = admin.device('Fruen', 'auto')
    a = admin.device('Anne')
    call(a, 'Fruen')
    assert_media_flows(a, frame)
    before = frame.evaluate('window.__streams.length')
    frame.evaluate('window.__muteLive()')  # like iOS interrupting capture
    frame.wait_for_function(f'window.__streams.length>{before}', timeout=15000)
    frame.wait_for_function('window.__streams.slice(0,-1).every(s=>s.getTracks().every(t=>t.readyState==="ended"))', timeout=5000)
    assert frame.is_visible('#visit')
    assert_media_flows(a, frame)  # picture and sound continue with the new tracks
    a.click('#hangup')
    assert_back_to_photos(frame)


def test_frame_reports_camera_that_stays_stopped(admin):
    frame = admin.device('Fruen', 'auto')
    a = admin.device('Anne')
    frame.evaluate('window.__forceMuted=10')  # every attempt gets muted tracks
    call(a, 'Fruen')
    a.wait_for_selector('#state:has-text("Fruen sin enhet fikk ikke startet kamera")', timeout=25000)
    frame.wait_for_function('window.__live()===0', timeout=5000)


def test_remote_reload_from_admin_and_automatic_reactivation(admin, base):
    frame = admin.device('Fruen', 'auto')
    a = admin.device('Anne')
    page = admin.ctx.new_page()
    page.goto('/fruen/admin')
    page.wait_for_selector('li:has-text("Fruen") >> text=klar for besøk', timeout=10000)
    frame.evaluate('window.__marker=1')
    page.once('dialog', lambda d: d.accept())
    page.click('li:has-text("Fruen") >> text=Last inn på nytt')
    page.wait_for_selector('#status:has-text("Sendt")')
    frame.wait_for_function('window.__marker===undefined', timeout=15000)  # the page was reloaded
    frame.wait_for_selector('#photo[src]:not([hidden])', timeout=15000)
    assert frame.is_hidden('#setup')  # nobody tapped anything
    call(a, 'Fruen')  # waits until the frame reports itself ready again
    assert_media_flows(a, frame)
    a.click('#hangup')
    assert_back_to_photos(frame)
    page.close()


def test_admin_shows_open_but_not_activated(admin):
    frame = admin.device('Fruen', 'auto', activate=False)
    frame.goto('/fruen')  # photos only, never activated on this device
    page = admin.ctx.new_page()
    page.goto('/fruen/admin')
    page.wait_for_selector('li:has-text("Fruen") >> text=bildene vises, men besøk er ikke aktivert', timeout=10000)
    page.close()


def test_slow_camera_does_not_stop_heartbeat(admin):
    a = admin.device('Anne')
    b = admin.device('Bjørn')
    b.evaluate('window.__gumDelay=9000')  # caller's camera starts slowly, under the 10 s limit
    call(b, 'Anne')
    a.click('#answer')
    assert_media_flows(a, b)
    b.click('#hangup')
    assert_back_to_photos(a)
    assert_back_to_photos(b)


def test_new_call_while_old_setup_is_pending(admin):
    a = admin.device('Anne')
    b = admin.device('Bjørn')
    c = admin.device('Cecilie')
    b.evaluate('window.__gumDelay=7000')  # B's camera for call 1 is slow
    call(b, 'Anne')
    a.click('#answer')
    b.wait_for_selector('#visit', state='visible', timeout=10000)
    time.sleep(1)
    b.click('#hangup')  # call 1 ends while B still waits for its camera
    a.wait_for_selector('#visit', state='hidden', timeout=10000)
    b.evaluate('window.__gumDelay=4000')
    call(b, 'Cecilie')  # call 2 starts; the stale setup resolves during it
    c.click('#answer')
    assert_media_flows(b, c)
    time.sleep(5)
    assert b.evaluate('window.__pcs.filter(pc=>pc.connectionState!=="closed").length') == 1
    c.click('#hangup')
    assert_back_to_photos(b)
    assert_back_to_photos(c)


def test_hidden_page_and_network_loss_end_visit(admin):
    a = admin.device('Anne')
    b = admin.device('Bjørn')
    call(a, 'Bjørn')
    b.click('#answer')
    assert_media_flows(a, b)
    b.evaluate("Object.defineProperty(document,'hidden',{configurable:true,get:()=>true});document.dispatchEvent(new Event('visibilitychange'))")
    b.wait_for_function('window.__live()===0', timeout=5000)
    assert_back_to_photos(a)
    b.evaluate("Object.defineProperty(document,'hidden',{configurable:true,get:()=>false});document.dispatchEvent(new Event('visibilitychange'))")
    call(b, 'Anne')
    a.click('#answer')
    assert_media_flows(a, b)
    a.context.set_offline(True)
    a.wait_for_function('window.__live()===0', timeout=20000)
    a.wait_for_selector('#state:has-text("Forbindelsen")')
    b.wait_for_selector('#visit', state='hidden', timeout=30000)
    assert live(b) == 0
    a.context.set_offline(False)


def test_frame_shows_no_connection_message_and_member_message_clears(admin):
    frame = admin.device('Fruen', 'auto')
    a = admin.device('Anne')
    for page in (frame, a):
        page.route('**/fruen/api/state', lambda route: route.abort())
    shown = lambda page: page.is_visible('#state') and 'borte' in page.text_content('#state')
    time.sleep(6)  # several failed polls, like a short restart of the server
    assert not shown(frame) and not shown(a)
    a.evaluate('Date.now=(n=>()=>n()+40000)(Date.now)')  # member: more than 30 s without contact
    a.wait_for_selector('#state:has-text("midlertidig borte")', timeout=6000)
    frame.evaluate('Date.now=(n=>()=>n()+900000)(Date.now)')  # frame: even after 15 min, nothing
    time.sleep(3)
    assert not shown(frame)
    for page in (frame, a):
        page.unroute('**/fruen/api/state')
    a.wait_for_function('document.getElementById("state").hidden', timeout=6000)  # cleared once contact is back
    frame.wait_for_selector('#photo[src]:not([hidden])')


def test_denied_camera_revoked_device_and_used_invite(admin, browser, base):
    a = admin.device('Anne', activate=False)
    a.evaluate('window.__gumDeny=true')
    a.click('#activate')
    a.wait_for_selector('#setupError:not(:empty)')
    assert a.is_visible('#setup') and a.is_disabled('#openContacts')
    b = admin.device('Bjørn')
    account = next(x for x in admin.ctx.request.get('/fruen/api/admin/accounts').json()['accounts']
                   if x['name'] == 'Bjørn' and x['active'])
    admin.ctx.request.post('/fruen/api/admin/revoke/' + account['id'], headers=HEAD, data={})
    b.wait_for_selector('#welcomeText:has-text("koble denne enheten til igjen")', timeout=10000)
    assert b.is_hidden('#photo') and b.is_hidden('#controls') and live(b) == 0
    token = admin('Engang')
    for expected in (None, 'brukt eller utløpt'):
        ctx = browser.new_context(base_url=base)
        page = ctx.new_page()
        page.goto('/fruen#invite=' + token)
        page.click('#enrol')
        if expected:
            page.wait_for_selector(f'#welcomeText:has-text("{expected}")')
        else:
            page.wait_for_selector('#welcome', state='hidden')
        ctx.close()


def test_night_mode(admin):
    frame = admin.device('Fruen', 'auto')
    a = admin.device('Anne')
    night = lambda on: admin.ctx.request.post(f'/fruen/api/test/night?on={on}')
    try:
        frame.wait_for_selector('#photo[src]:not([hidden])')
        # A visit in progress when night starts continues; the screen goes dark afterwards.
        call(a, 'Fruen')
        assert_media_flows(a, frame)
        night(1)
        time.sleep(5)
        assert frame.is_visible('#visit') and frame.is_hidden('#night')
        a.click('#hangup')
        frame.wait_for_selector('#night', state='visible', timeout=10000)
        assert live(frame) == 0
        a.click('#openContacts')
        a.wait_for_selector('#contacts button:has-text("Sover")[disabled]', timeout=10000)
        a.click('#closeContacts')
        frame.goto('/fruen/klargjor')  # a relative re-activates the frame at night
        frame.click('#activate', timeout=10000)
        frame.wait_for_selector('#setup', state='hidden')
        frame.wait_for_selector('#night', state='visible', timeout=10000)
        night(0)
        frame.wait_for_selector('#night', state='hidden', timeout=10000)
        frame.wait_for_selector('#photo[src]:not([hidden])', timeout=15000)
        time.sleep(4)  # two polls: the frame reports itself ready again
        call(a, 'Fruen')
        assert_media_flows(a, frame)
        a.click('#hangup')
        assert_back_to_photos(frame)
    finally:
        night(0)


@pytest.mark.parametrize('size', [(820, 1180), (1180, 820), (375, 667)])
def test_layout_screenshots(admin, size, tmp_path):
    a = admin.device('Anne', viewport={'width': size[0], 'height': size[1]})
    frame = admin.device('Fruen', 'auto', viewport={'width': size[0], 'height': size[1]})
    out = Path(os.environ.get('FRUEN_SCREENSHOTS', tmp_path))
    out.mkdir(parents=True, exist_ok=True)
    frame.wait_for_selector('#photo[src]:not([hidden])')
    frame.screenshot(path=str(out / f'frame-{size[0]}x{size[1]}.png'))
    call(a, 'Fruen')
    frame.wait_for_selector('#ringing', state='visible')
    frame.screenshot(path=str(out / f'ringing-{size[0]}x{size[1]}.png'))
    assert_media_flows(a, frame)
    a.screenshot(path=str(out / f'visit-member-{size[0]}x{size[1]}.png'))
    for page in (a, frame):
        overflow = page.evaluate('document.documentElement.scrollWidth>innerWidth')
        assert not overflow
    box = a.locator('#hangup').bounding_box()
    assert box and box['x'] + box['width'] <= size[0] and box['y'] + box['height'] <= size[1]
    a.click('#hangup')
    assert_back_to_photos(frame)
