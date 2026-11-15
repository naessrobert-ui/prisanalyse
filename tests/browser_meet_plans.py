"""Local browser regression using preview_meet_plans; no real emails or AI calls."""
import os
from datetime import datetime,timedelta
from pathlib import Path
from urllib.parse import urlsplit
from playwright.sync_api import sync_playwright,expect

expect.set_options(timeout=25000)
base=os.environ.get('MEET_PLANS_TEST_URL','http://localhost:8774/')
with sync_playwright() as p:
    launch={'executable_path':'/usr/bin/chromium','headless':True,'args':['--no-sandbox','--use-fake-ui-for-media-stream','--use-fake-device-for-media-stream']}
    if base.startswith('https:'):
        q=urlsplit(os.environ.get('HTTPS_PROXY') or os.environ.get('https_proxy') or '')
        if q.hostname:launch['proxy']={'server':q.scheme+'://'+q.hostname+(':'+str(q.port) if q.port else '')}
        if q.username:launch['proxy']['username']=q.username
        if q.password:launch['proxy']['password']=q.password
    browser=p.chromium.launch(**launch)
    context=browser.new_context(ignore_https_errors=True,permissions=['microphone'],viewport={'width':1440,'height':1050})
    context.add_init_script('''window.testCopies=[];window.testMicCalls=0;Object.defineProperty(navigator,'share',{value:undefined});Object.defineProperty(navigator,'clipboard',{value:{writeText:async text=>{window.testCopies.push(text);}}});const original=navigator.mediaDevices.getUserMedia.bind(navigator.mediaDevices);navigator.mediaDevices.getUserMedia=(...args)=>{window.testMicCalls++;return original(...args);};''')
    errors=[];a=context.new_page();a.on('pageerror',lambda e:errors.append(str(e)));a.on('dialog',lambda d:d.accept());a.goto(base)
    a.locator('#planMode').click();expect(a.locator('#scheduleFields')).to_be_visible();expect(a.locator('#roomWrap')).not_to_be_visible();expect(a.locator('#joinBtn')).to_be_enabled(timeout=60000)
    a.locator('#nameIn').fill('Planleggingstest');a.locator('#planTitle').fill('Teknisk test – planlagt møte')
    date=(datetime.now()+timedelta(days=15)).strftime('%Y-%m-%dT12:30');a.locator('#planStart').fill(date);a.locator('#planAgenda').fill('Sak 1: kalender\nSak 2: oppgaver');a.locator('#joinBtn').click()
    expect(a.locator('#planResult')).to_be_visible();join=a.locator('#planJoinUrl').input_value();manage=a.locator('#planOwnerUrl').input_value();secret=manage.rsplit('.',1)[1]
    assert secret not in join and a.evaluate('window.testMicCalls')==0
    with a.expect_download() as saved:a.locator('#planCalendar').click()
    text=Path(saved.value.path()).read_text();assert 'BEGIN:VCALENDAR' in text and secret not in text and 'UID:plan-' in text
    a.locator('#planShare').click();assert join in a.evaluate('window.testCopies.at(-1)') and secret not in a.evaluate('window.testCopies.at(-1)')
    a.locator('#planEdit').click();a.locator('#planTitle').fill('Oppdatert teknisk test');a.locator('#planDuration').select_option('90');a.locator('#joinBtn').click();expect(a.locator('#planTitleOut')).to_have_text('Oppdatert teknisk test');assert a.locator('#planJoinUrl').input_value()==join
    a.locator('#planClose').click();a.locator('#myPlans').click();expect(a.locator('#plansList')).to_contain_text('Oppdatert teknisk test');assert a.locator('#plansList button').count()==1;a.locator('#plansList button').click();expect(a.locator('#planResult')).to_be_visible()
    guest_context=browser.new_context(ignore_https_errors=True,permissions=['microphone']);b=guest_context.new_page();b.on('pageerror',lambda e:errors.append(str(e)));b.goto(join);expect(b.locator('#invitedNote')).to_contain_text('Oppdatert teknisk test');b.locator('#guestCalendar').click();expect(b.locator('#planEdit')).not_to_be_visible();expect(b.locator('#planOwner')).not_to_be_visible();b.locator('#planClose').click();b.locator('#nameIn').fill('Tidlig deltaker');b.locator('#joinBtn').click();expect(b.locator('#app')).to_be_visible()
    a.locator('#planCancel').click();expect(a.locator('#planCancelled')).to_be_visible()
    c=guest_context.new_page();c.goto(join);expect(c.locator('#invitedNote')).to_contain_text('avlyst');c.locator('#nameIn').fill('Ny deltaker');c.locator('#joinBtn').click();expect(c.locator('#landNote')).to_contain_text('avlyst');expect(c.locator('#app')).not_to_be_visible()
    b.locator('#leaveBtn').click()
    other=browser.new_context(ignore_https_errors=True);d=other.new_page();d.on('dialog',lambda x:x.accept());d.goto(manage);expect(d.locator('#planTitleOut')).to_have_text('Oppdatert teknisk test');expect(d.locator('#planCancelled')).to_be_visible()
    with d.expect_download() as cancelled:d.locator('#planCalendar').click()
    assert 'METHOD:CANCEL' in Path(cancelled.value.path()).read_text()
    d.locator('#planOwner summary').click();d.locator('#planDelete').click();expect(d.locator('#landNote')).to_contain_text('slettet')
    a.locator('#planClose').click();a.set_viewport_size({'width':390,'height':844});a.locator('#planMode').click();assert a.evaluate('document.documentElement.scrollWidth<=innerWidth')
    assert not errors,errors;browser.close()
    print('PASS: future scheduling without microphone, calendar download, private owner link, sharing, edit, local list, early guest entry, cancellation, owner recovery, deletion and mobile.')
