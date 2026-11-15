"""Run preview_meeting_minutes on 127.0.0.1:8772 first. No real API use."""
from playwright.sync_api import sync_playwright,expect
expect.set_options(timeout=20000)
with sync_playwright() as p:
    browser=p.chromium.launch(executable_path='/usr/bin/chromium',headless=True,args=['--no-sandbox','--use-fake-ui-for-media-stream','--use-fake-device-for-media-stream'])
    context=browser.new_context(permissions=['microphone'],viewport={'width':1440,'height':1050})
    a=context.new_page();errors=[];a.on('pageerror',lambda e:errors.append(str(e)));a.on('dialog',lambda d:d.accept())
    a.goto('http://localhost:8772/');a.locator('#nameIn').fill('Møteleder');a.locator('#joinBtn').click();expect(a.locator('#app')).to_be_visible()
    b=context.new_page();b.on('pageerror',lambda e:errors.append(str(e)));b.goto(a.url);b.locator('#nameIn').fill('Deltaker');b.locator('#joinBtn').click();expect(b.locator('#app')).to_be_visible();a.bring_to_front()
    expect(a.locator('#minutesStart')).to_be_enabled();a.locator('#minutesStart').click()
    expect(a.locator('#minutesStatus')).to_contain_text('Møtelyden transkriberes');expect(b.locator('#minutesStatus')).to_contain_text('Møtelyden transkriberes')
    assert a.request.get('http://localhost:8772/testing/stats').json()['chunks']==0
    # Real AudioWorklet, mixed WebRTC/microphone audio and generated WAV go through signed Pi validation.
    with a.expect_response('**/meet/minutes/chunk/0',timeout=45000) as uploaded:
        a.wait_for_timeout(30500)
    assert uploaded.value.status==200
    stats=a.request.get('http://localhost:8772/testing/stats').json();assert stats['chunks']==1 and stats['audio_bytes']>600000 and stats['summaries']==0
    a.locator('#minutesStop').click();expect(a.locator('#minutesOpen')).to_be_visible();a.locator('#minutesOpen').click()
    expect(a.locator('#minutesDraft')).to_have_value(__import__('re').compile('REFERATUTKAST'),timeout=25000)
    expect(b.locator('#minutesOpen')).to_be_visible();b.locator('#minutesOpen').click();expect(b.locator('#minutesDraft')).to_have_value(__import__('re').compile('REFERATUTKAST'))
    assert a.request.get('http://localhost:8772/testing/stats').json()['summaries']==1
    with a.expect_download() as saved:a.locator('#minutesDownload').click()
    assert saved.value.suggested_filename=='referatutkast.txt'
    a.locator('#minutesClose').click();b.locator('#minutesClose').click();a.locator('#leaveBtn').click();b.locator('#leaveBtn').click()
    # Reload/recovery works without an active meeting; no repeated AI summary.
    a.reload();expect(a.locator('#minutesLast')).to_be_visible();a.locator('#minutesLast').click();expect(a.locator('#minutesDraft')).to_have_value(__import__('re').compile('REFERATUTKAST'))
    assert a.request.get('http://localhost:8772/testing/stats').json()['summaries']==1
    a.set_viewport_size({'width':390,'height':844});assert a.evaluate('document.documentElement.scrollWidth<=innerWidth')
    assert not errors,errors
    browser.close();print('PASS: visible notice, no approval gate, real AudioWorklet mixed WAV upload, local queue, one final summary, participant sharing, download, recovery and mobile.')
