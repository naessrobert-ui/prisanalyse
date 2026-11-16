(()=>{'use strict';
const $=id=>document.getElementById(id);
let me=null,armed=false,context=null,wake=null,photos=[],bag=[],currentPhoto=null,photoTimer=null,showToken=0,pollTimer=null,lastPoll=Date.now(),polling=false;
let night=false;
let call=null,pc=null,pcCall=null,peerState=null,local=null,remote=null,remoteAudio=null,signalSeq=0,ice=[];
let autoTimer=null,ringTimer=null,connectionTimer=null,disconnectTimer=null,statusTimer=null,setupBusy=false,contacts=[],pendingToken=null,acceptingId=null;
const ignored=new Set();
const speaker=document.createElement('audio');speaker.setAttribute('playsinline','');speaker.hidden=true;document.body.append(speaker);
let clipSound=false;try{clipSound=localStorage.getItem('fruen-clip-sound')==='1';}catch(e){}$('clipSound').checked=clipSound;

async function api(path,data){const r=await fetch('/fruen/api/'+path,{method:data===undefined?'GET':'POST',headers:{'Content-Type':'application/json','X-Fruen-Request':'1'},body:data===undefined?undefined:JSON.stringify(data)});let v;try{v=await r.json();}catch(e){throw Error('Besøkstjenesten svarer ikke.');}if(!r.ok){const e=Error(v.detail||'Kunne ikke fullføre.');e.status=r.status;throw e;}return v;}
// Short confirmations fade out so the picture frame stays calm; problems stay visible.
// On her frame only the 'needs re-activation' notice stays; everything else fades.
let lostSince=0,connectionNotice=false,reloadWanted=false;
function status(text,sticky=false,keep=false){if(sticky&&!keep&&me?.kind==='auto')sticky=false;connectionNotice=false;clearTimeout(statusTimer);const box=$('state');box.textContent=text;box.hidden=false;if(!sticky)statusTimer=setTimeout(()=>{box.hidden=true;},8000);}
function stopStream(stream){stream?.getTracks().forEach(t=>t.stop());}
function sound(){if(!context||context.state!=='running')return;for(const [frequency,delay] of [[523,0],[659,.2]]){const o=context.createOscillator(),g=context.createGain(),start=context.currentTime+delay;o.frequency.value=frequency;g.gain.setValueAtTime(.045,start);g.gain.exponentialRampToValueAtTime(.001,start+.25);o.connect(g);g.connect(context.destination);o.start(start);o.stop(start+.26);o.onended=()=>{o.disconnect();g.disconnect();};}}
function ringSound(){if(ringTimer)return;context?.resume().catch(()=>{});sound();ringTimer=setInterval(sound,3000);}
function stopRing(){clearInterval(ringTimer);ringTimer=null;clearTimeout(autoTimer);autoTimer=null;}

// ---- Slideshow -------------------------------------------------------------
function shuffled(items){const a=[...items];for(let i=a.length-1;i>0;i--){const j=Math.floor(Math.random()*(i+1));[a[i],a[j]]=[a[j],a[i]];}return a;}
function pausePhotos(){showToken++;clearTimeout(photoTimer);photoTimer=null;$('clip').pause();}
function resetClip(){const clip=$('clip');clip.onended=clip.onerror=clip.onloadeddata=null;clip.pause();clip.removeAttribute('src');clip.load();clip.hidden=true;}
function nextPhoto(){
 if(call&&call.status==='accepted')return;
 pausePhotos();if(night)return;const token=showToken;
 if(!photos.length){currentPhoto=null;resetClip();$('photo').hidden=true;$('photo').removeAttribute('src');$('empty').hidden=false;$('caption').textContent='';return;}
 if(!bag.length){bag=shuffled(photos);if(bag.length>1&&bag[0].id===currentPhoto?.id)bag.push(bag.shift());}
 const item=bag.shift();currentPhoto=item;const url='/fruen/media/'+item.id;
 // The previous picture stays on screen until the next one has loaded, so a network
 // outage freezes the frame instead of showing a broken image.
 const failed=()=>{if(token!==showToken)return;clearTimeout(photoTimer);photoTimer=setTimeout(nextPhoto,5000);};
 const shown=()=>{$('empty').hidden=true;$('caption').textContent=item.caption||'';};
 if(item.mime.startsWith('video/')){
  const clip=$('clip');resetClip();clip.muted=!clipSound;
  clip.onloadeddata=()=>{if(token!==showToken)return;$('photo').hidden=true;$('photo').removeAttribute('src');clip.hidden=false;shown();};
  clip.onended=()=>{if(token===showToken)nextPhoto();};clip.onerror=failed;clip.src=url;
  clip.play().catch(()=>{if(token!==showToken)return;clip.muted=true;clip.play().catch(failed);});
  photoTimer=setTimeout(()=>{if(token===showToken)nextPhoto();},300000);
 }else{
  const img=new Image();img.decoding='async';
  img.onload=()=>{if(token!==showToken)return;resetClip();$('photo').src=url;$('photo').hidden=false;shown();clearTimeout(photoTimer);photoTimer=setTimeout(nextPhoto,12000);};
  img.onerror=failed;img.src=url;photoTimer=setTimeout(failed,20000);
 }
}
async function loadPhotos(){if(!me)return;try{const all=[];for(let offset=0;offset<=100000;offset+=200){const page=await api('photos?offset='+offset);all.push(...page.items);if(!page.more)break;}photos=[...new Map(all.map(item=>[item.id,item])).values()];const ids=new Set(photos.map(p=>p.id));bag=bag.filter(p=>ids.has(p.id));if(!currentPhoto||!ids.has(currentPhoto.id))nextPhoto();}catch(e){if(e.status===401)locked();}}

// ---- Camera, microphone and sound ------------------------------------------
function getMedia(expected){return new Promise((resolve,reject)=>{let done=false;const timer=setTimeout(()=>{done=true;reject(Error('Kamera eller mikrofon svarte ikke. En pårørende må kontrollere tillatelsene.'));},10000);navigator.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true},video:{width:{ideal:640},height:{ideal:480},frameRate:{ideal:15,max:24}}}).then(stream=>{clearTimeout(timer);if(done||document.hidden||(expected&&call?.id!==expected)){stopStream(stream);if(!done){done=true;reject(Error('Samtalen er avsluttet.'));}return;}done=true;resolve(stream);},error=>{clearTimeout(timer);if(!done){done=true;reject(error);}});});}
async function activate(){
 fullscreen();if(setupBusy)return;setupBusy=true;$('activate').disabled=true;$('setupError').textContent='';
 try{
  if(!navigator.mediaDevices?.getUserMedia)throw Error('Denne nettleseren har ikke tilgang til kamera og mikrofon.');
  if(!context||context.state==='closed')context=new(window.AudioContext||window.webkitAudioContext)();
  context.resume().catch(()=>{});const sample=await healthyMedia(null);
  // iOS may pause audio while the camera switches on. Try to resume while capture is
  // still active, but never let the notification sound block the visit itself.
  await Promise.race([context.resume().catch(()=>{}),new Promise(r=>setTimeout(r,1500))]);stopStream(sample);
  clipSound=$('clipSound').checked;try{localStorage.setItem('fruen-clip-sound',clipSound?'1':'0');}catch(e){}
  armed=true;context.onstatechange=()=>{if(armed&&context.state!=='running')context.resume().catch(()=>{});};
  if(me?.kind==='auto')try{localStorage.setItem('fruen-armed','1');}catch(e){}
  requestWake();
  $('setup').hidden=true;history.replaceState(null,'','/fruen');sound();status('Besøk er aktivert.');renderControls();poll();
  setTimeout(()=>{if(armed&&me?.kind==='auto'&&!isFullscreen()&&(document.documentElement.requestFullscreen||document.documentElement.webkitRequestFullscreen))status('Trykk én gang på bildet for fullskjerm.');},1500);
 }catch(e){$('setupError').textContent=e.message||'Kontroller tillatelser til lyd, kamera og mikrofon.';}
 finally{setupBusy=false;$('activate').disabled=false;}
}
// Fullscreen needs a tap. Only the automatic frame uses it; a relative's tap on the
// activation button (or once on the picture afterwards) is enough. Leaves on reload.
function isFullscreen(){return !!(document.fullscreenElement||document.webkitFullscreenElement);}
function fullscreen(){if(me?.kind!=='auto'||isFullscreen())return;const el=document.documentElement,request=el.requestFullscreen||el.webkitRequestFullscreen;if(!request)return;try{const r=request.call(el);if(r&&r.catch)r.catch(()=>{});}catch(e){}}
// iOS can give Safari camera/microphone tracks that are 'muted' (capture interrupted,
// e.g. another app took them). Wait briefly, try once more, then report it instead of
// sending a black, silent picture.
function healthy(stream){const tracks=stream?.getTracks()||[];return tracks.length>0&&tracks.every(t=>t.readyState==='live'&&!t.muted);}
async function healthyMedia(expected){
 for(let attempt=0;attempt<2;attempt++){
  const stream=await getMedia(expected);
  for(let i=0;i<8&&!healthy(stream);i++)await new Promise(r=>setTimeout(r,250));
  if(healthy(stream))return stream;
  stopStream(stream);
  if(expected&&call?.id!==expected)throw Error('Samtalen er avsluttet.');
 }
 throw Error('Kamera eller mikrofon er stoppet av enheten. Lukk andre apper som bruker kamera eller lyd, og prøv igjen.');
}
let localTimer=null;
function watchLocal(connection,id){
 clearInterval(localTimer);let bad=0,recovered=false,busy=false;
 localTimer=setInterval(async()=>{
  if(pc!==connection){clearInterval(localTimer);return;}
  if(busy)return;if(healthy(local)){bad=0;return;}
  if(++bad<2)return;
  if(recovered){clearInterval(localTimer);endCall('camera');return;}
  recovered=true;bad=0;busy=true;
  try{
   const fresh=await healthyMedia(id);if(pc!==connection){stopStream(fresh);return;}
   for(const sender of connection.getSenders()){const kind=sender.track?.kind;const track=kind&&fresh.getTracks().find(t=>t.kind===kind);if(track)await sender.replaceTrack(track);}
   const old=local;local=fresh;stopStream(old);if(me.kind==='member')$('local').srcObject=local;
  }catch(e){if(pc===connection)endCall('camera');}
  finally{busy=false;}
 },2000);
}
function requestWake(){if(armed&&navigator.wakeLock&&!document.hidden)navigator.wakeLock.request('screen').then(v=>{wake=v;}).catch(()=>{});}
// Remote sound plays through a media element (echo cancellation, and Safari allows
// it while the page captures). Web Audio is only a fallback when playback is refused.
function playRemoteAudio(track,connection){
 remoteAudio?.disconnect();remoteAudio=null;speaker.srcObject=new MediaStream([track]);speaker.muted=false;
 speaker.play().then(()=>{if(pc!==connection){speaker.pause();speaker.srcObject=null;}}).catch(()=>{
  if(pc!==connection)return;speaker.srcObject=null;
  if(context?.state==='running'){remoteAudio=context.createMediaStreamSource(new MediaStream([track]));remoteAudio.connect(context.destination);}
 });
}

// ---- Visits ----------------------------------------------------------------
// Night mode (automatic frame only): black screen, no new calls. A visit already in
// progress continues; the screen goes dark when the caller hangs up.
function renderNight(){$('night').hidden=!(night&&!(call&&call.status==='accepted'));}
function setNight(on){if(on===night){renderNight();return;}night=on;renderNight();if(night){if(!(call&&call.status==='accepted'))pausePhotos();}else nextPhoto();}
function renderControls(){const interactive=me?.kind==='member';$('controls').hidden=!interactive;$('resume').hidden=armed;$('openContacts').disabled=!armed;$('hangup').hidden=!call||call.status==='ended';}
function contactList(){const box=$('contacts');box.replaceChildren();for(const person of contacts){const button=document.createElement('button');const title=document.createElement('span');title.textContent=person.name;const subtitle=document.createElement('small');subtitle.textContent=person.sleeping?'Sover (nattmodus 21–08)':person.online?'Klar for besøk':'Ikke tilkoblet';button.append(title,subtitle);button.disabled=!person.online||!!call;button.onclick=()=>startCall(person);box.append(button);}if(!contacts.length){const p=document.createElement('p');p.textContent='Ingen andre enheter er invitert ennå.';box.append(p);}}
async function startCall(person){if(call)return;try{context?.resume().catch(()=>{});const v=await api('call',{to:person.id});call={id:v.id,status:'ringing',outgoing:true,peer_name:person.name};signalSeq=0;showRinging(call);$('contactDialog').close();poll();}catch(e){status(e.message,true);}}
function showRinging(value){
 $('ringing').hidden=false;$('ringName').textContent=value.outgoing?'Ringer '+value.peer_name:value.peer_name+' kommer på besøk';$('ringText').textContent=value.outgoing?'Venter på svar …':me.kind==='auto'?'Et lite øyeblikk. Samtalen starter automatisk.':'Vil du svare?';
 $('answer').hidden=value.outgoing||me.kind==='auto';$('decline').hidden=me.kind==='auto';$('decline').textContent=value.outgoing?'Avbryt':'Avvis';ringSound();renderControls();
 if(!value.outgoing&&me.kind==='auto'&&!autoTimer&&acceptingId!==value.id){const id=value.id;autoTimer=setTimeout(()=>{autoTimer=null;if(call?.id===id&&armed&&!document.hidden)acceptCall();},5000);}
}
async function acceptCall(){const id=call?.id;if(!id||acceptingId===id||call.outgoing)return;acceptingId=id;$('answer').disabled=true;try{context?.resume().catch(()=>{});const stream=await healthyMedia(id);if(call?.id!==id){stopStream(stream);return;}stopStream(local);local=stream;await api('call/'+id+'/accept',{});if(call?.id===id){stopRing();poll();}}catch(e){if(call?.id===id){status('Besøket kunne ikke starte. Kontroller kamera og mikrofon.',true);endCall(e?.status?'connection':'camera');}}finally{$('answer').disabled=false;if(acceptingId===id)acceptingId=null;}}
// One peer connection per call. State is keyed by call id so a slow setup for an old
// call can never clear or replace the setup of a newer call.
async function peer(value){
 const id=value.id;
 if(pc&&pcCall===id)return;
 if(peerState?.id===id)return peerState.promise;
 const state={id};
 state.promise=(async()=>{
  const settings=await api('ice');if(call?.id!==id)return;
  if(!local){const source=await healthyMedia(id);if(call?.id!==id){stopStream(source);return;}local=source;}
  const connection=new RTCPeerConnection({iceServers:settings.iceServers});pc=connection;pcCall=id;remote=new MediaStream();$('remote').srcObject=remote;$('remote').play().catch(()=>{});
  local.getTracks().forEach(track=>connection.addTrack(track,local));watchLocal(connection,id);if(me.kind==='member'){$('local').hidden=false;$('local').srcObject=local;}
  connection.onicecandidate=e=>{if(e.candidate&&pc===connection)api('call/'+id+'/signal',{kind:'ice',data:e.candidate.toJSON()}).catch(()=>{});};
  connection.ontrack=e=>{if(pc!==connection)return;remote.addTrack(e.track);if(e.track.kind==='audio')playRemoteAudio(e.track,connection);};
  connection.onconnectionstatechange=()=>{if(pc!==connection)return;const s=connection.connectionState;if(s==='connected'){clearTimeout(connectionTimer);clearTimeout(disconnectTimer);clearTimeout(statusTimer);$('state').hidden=true;if(me.kind==='member')watchMedia(connection,value.peer_name);}else if(s==='failed'){status('Besøket mistet forbindelsen.',true);endCall();}else if(s==='disconnected'){clearTimeout(disconnectTimer);disconnectTimer=setTimeout(()=>{if(pc===connection&&connection.connectionState!=='connected'){status('Besøket mistet forbindelsen.',true);endCall();}},8000);}};
  connectionTimer=setTimeout(()=>{if(pc===connection&&connection.connectionState!=='connected'){status('Besøket fikk ikke forbindelse.',true);endCall('connection');}},20000);
  if(value.outgoing){const offer=await connection.createOffer();if(pc!==connection)return;await connection.setLocalDescription(offer);if(pc!==connection)return;await api('call/'+id+'/signal',{kind:'offer',data:{type:'offer',sdp:connection.localDescription.sdp}});}
 })();
 peerState=state;
 try{await state.promise;}finally{if(peerState===state)peerState=null;}
}
async function flushIce(connection){for(const c of ice.splice(0)){if(pc!==connection)return;await connection.addIceCandidate(c).catch(()=>{});}}
async function applySignals(value){
 for(const signal of value.signals||[]){
  const connection=pc;if(!connection||pcCall!==value.id||call?.id!==value.id)return;
  if(signal.seq<=signalSeq)continue;
  signalSeq=signal.seq; // advance first: a bad message must not be retried on every poll
  try{
   if(signal.kind==='offer'){
    if(value.outgoing||connection.remoteDescription)continue; // duplicate offer
    await connection.setRemoteDescription(signal.data);if(pc!==connection)return;await flushIce(connection);
    const answer=await connection.createAnswer();if(pc!==connection)return;await connection.setLocalDescription(answer);if(pc!==connection)return;
    await api('call/'+value.id+'/signal',{kind:'answer',data:{type:'answer',sdp:connection.localDescription.sdp}});
   }else if(signal.kind==='answer'){
    if(!value.outgoing||connection.signalingState!=='have-local-offer')continue; // duplicate answer
    await connection.setRemoteDescription(signal.data);if(pc!==connection)return;await flushIce(connection);
   }else if(connection.remoteDescription)await connection.addIceCandidate(signal.data).catch(()=>{});
   else ice.push(signal.data);
  }catch(e){if(pc===connection){status('Besøket fikk ikke forbindelse.',true);endCall('connection');}return;}
 }
}
async function processCall(value){
 if(!value||value.status==='ended'){if(call&&value&&value.id===call.id&&me?.kind==='member'){const text=endedText(value,call.status==='ringing');if(text)status(text,true);}if(call)stopCall();return;}
 if(ignored.has(value.id)){api('call/'+value.id+'/end',{}).catch(()=>{});return;}
 if(call?.id!==value.id){if(call)stopCall();call=value;signalSeq=0;}else call=value;
 if(value.status==='ringing'){showRinging(value);return;}
 stopRing();$('ringing').hidden=true;$('visit').hidden=false;$('callTitle').textContent='Besøk fra '+value.peer_name;pausePhotos();renderControls();
 // Camera start-up can be slow. It must not block polling, or the heartbeat stops and
 // the visit is ended. Unapplied signals are delivered again on the next poll.
 if(!(pc&&pcCall===value.id)){
  if(peerState?.id!==value.id)peer(value).then(()=>{if(call?.id===value.id)poll();},e=>{if(call?.id===value.id){status('Besøket kunne ikke starte. Kontroller kamera og mikrofon.',true);endCall(e&&/^(NotAllowed|NotReadable|NotFound|Overconstrained|Abort|Security)Error$/.test(e.name)||/Kamera/.test(e?.message||'')?'camera':'connection');}});
  return;
 }
 await applySignals(value);
}
// Tells the caller what is wrong with the picture or sound from the other side, so a
// black screen can be told apart: nothing sent, sent but dark/covered, or no sound.
let mediaTimer=null;
function watchMedia(connection,who){
 clearInterval(mediaTimer);const canvas=document.createElement('canvas');canvas.width=32;canvas.height=18;const g=canvas.getContext('2d',{willReadFrequently:true});
 let last=null,noVideo=0,dark=0,noAudio=0,shown='';
 mediaTimer=setInterval(async()=>{
  if(pc!==connection){clearInterval(mediaTimer);return;}
  const now={video:0,audio:0};try{(await connection.getStats()).forEach(r=>{if(r.type==='inbound-rtp'){if(r.kind==='video')now.video=r.framesDecoded||0;else if(r.kind==='audio')now.audio=r.bytesReceived||0;}});}catch(e){return;}
  if(last){noVideo=now.video>last.video?0:noVideo+1;noAudio=now.audio>last.audio?0:noAudio+1;}last=now;
  let light=255;const v=$('remote');if(v.videoWidth&&noVideo===0){try{g.drawImage(v,0,0,32,18);const d=g.getImageData(0,0,32,18).data;let sum=0;for(let i=0;i<d.length;i+=4)sum+=0.2126*d[i]+0.7152*d[i+1]+0.0722*d[i+2];light=sum/(d.length/4);}catch(e){}}
  dark=light<14?dark+1:0;
  const parts=[];
  if(noVideo>=3)parts.push('Ingen bilde fra '+who+'. Kameraet der sender ikke: det kan være i bruk av en annen app, stoppet av enheten, eller skjermen kan være låst.');
  else if(dark>=3)parts.push('Bildet fra '+who+' er helt svart eller svært mørkt. Er kameraet dekket til, eller er det mørkt i rommet?');
  if(noAudio>=3)parts.push('Ingen lyd fra '+who+'.');
  const text=parts.join(' ');
  if(text!==shown){shown=text;if(text)status(text,true);else{clearTimeout(statusTimer);$('state').hidden=true;}}
 },2000);
}
function stopCall(){
 clearInterval(mediaTimer);mediaTimer=null;clearInterval(localTimer);localTimer=null;
 stopRing();clearTimeout(connectionTimer);clearTimeout(disconnectTimer);
 if(pc){pc.ontrack=null;pc.onicecandidate=null;pc.onconnectionstatechange=null;pc.close();pc=null;}
 pcCall=null;peerState=null;stopStream(local);local=null;stopStream(remote);remote=null;remoteAudio?.disconnect();remoteAudio=null;speaker.pause();speaker.srcObject=null;
 ice=[];acceptingId=null;call=null;signalSeq=0;
 $('remote').srcObject=null;$('local').srcObject=null;$('local').hidden=true;$('visit').hidden=true;$('ringing').hidden=true;renderControls();renderNight();nextPhoto();
}
function endCall(reason='hangup'){const id=call?.id;if(id){ignored.add(id);if(ignored.size>20)ignored.delete(ignored.values().next().value);}stopCall();if(id)api('call/'+id+'/end',{reason}).catch(()=>{});}
// Why a visit ended, told to the person who did not end it.
function endedText(value,wasRinging){
 const [side,code]=String(value.reason||'').split(':'),who=value.peer_name,mine=(side==='caller')===!!value.outgoing;
 if(mine)return '';
 if(code==='camera')return who+' sin enhet fikk ikke startet kamera eller mikrofon.'+(value.peer_kind==='auto'?' Noen hos henne må sette Kamera og Mikrofon til «Tillat» i Safari.':'');
 if(code==='declined')return who+' kan ikke ta imot besøk akkurat nå.';
 if(code==='hidden')return 'Skjermen til '+who+' ble slått av eller lukket.';
 if(code==='connection')return 'Besøket fikk ikke forbindelse med '+who+'.';
 if(code==='noanswer')return who+' svarte ikke.';
 if(side==='timeout')return wasRinging?who+' svarte ikke.':'Forbindelsen til '+who+' ble brutt.';
 return code==='hangup'&&!wasRinging?who+' la på.':'';
}
function locked(){armed=false;try{localStorage.removeItem('fruen-armed');}catch(e){}endCall();pausePhotos();resetClip();$('photo').removeAttribute('src');$('photo').hidden=true;photos=[];bag=[];currentPhoto=null;me=null;$('setup').hidden=true;$('controls').hidden=true;$('welcome').hidden=false;$('enrol').hidden=true;$('welcomeText').textContent='En pårørende må koble denne enheten til igjen.';status('Besøk er ikke tilgjengelig.',true);}
async function poll(){
 if(!me||polling)return;clearTimeout(pollTimer);polling=true;
 try{const value=await api('state',{ready:armed&&!document.hidden&&(!night||call?.status==='accepted'),after:signalSeq});lastPoll=Date.now();lostSince=0;if(connectionNotice){connectionNotice=false;$('state').hidden=true;}setNight(!!value.night);if(value.reload)reloadWanted=true;if(reloadWanted&&!call){location.reload();return;}contacts=value.contacts;if($('contactDialog').open)contactList();if(armed&&!document.hidden)await processCall(value.call);else if(call)endCall();}
 catch(e){if(e.status===401)locked();else{if(!lostSince)lostSince=Date.now();if(me?.kind==='member'&&Date.now()-lostSince>30000&&!connectionNotice){status('Forbindelsen er midlertidig borte. Bildene vises videre.',true);connectionNotice=true;}}}
 finally{polling=false;if(me)pollTimer=setTimeout(poll,2000);}
}
// After a reload the frame activates itself again, but only if a relative activated it
// before on this device and Safari already allows camera and microphone (no prompt).
async function autoArm(){
 let wanted=false;try{wanted=localStorage.getItem('fruen-armed')==='1';}catch(e){}
 if(me?.kind!=='auto'||!wanted||!navigator.permissions?.query||!navigator.mediaDevices?.getUserMedia)return false;
 try{const states=await Promise.all(['camera','microphone'].map(name=>navigator.permissions.query({name})));if(states.some(s=>s.state!=='granted'))return false;}catch(e){return false;}
 try{if(!context||context.state==='closed')context=new(window.AudioContext||window.webkitAudioContext)();context.resume().catch(()=>{});context.onstatechange=()=>{if(armed&&context.state!=='running')context.resume().catch(()=>{});};}catch(e){}
 armed=true;requestWake();renderControls();return true;
}
async function boot(){
 try{me=await api('me');$('welcome').hidden=true;renderControls();await loadPhotos();if(me.kind==='member'||location.pathname.endsWith('/klargjor')){$('setup').hidden=false;$('setupTitle').textContent=me.kind==='auto'?'Klargjør Fruens bildeskjerm':'Aktiver familiebesøk';}else if(!await autoArm())status('Familiebilder · besøk må aktiveres av en pårørende etter ny innlasting.',true,true);poll();}
 catch(e){$('welcome').hidden=false;status('Privat familiebesøk',true);}
}
$('activate').onclick=activate;
document.addEventListener('pointerup',()=>{if(armed)fullscreen();});
$('resume').onclick=()=>{$('setup').hidden=false;};$('openContacts').onclick=()=>{contactList();$('contactDialog').showModal();};$('closeContacts').onclick=()=>$('contactDialog').close();$('hangup').onclick=()=>endCall('hangup');$('decline').onclick=()=>endCall(call&&!call.outgoing?'declined':'hangup');$('answer').onclick=acceptCall;
$('logout').onclick=async()=>{if(!confirm('Koble denne enheten fra familiebesøk?'))return;try{localStorage.removeItem('fruen-armed');}catch(e){}endCall();try{await api('logout',{});location.reload();}catch(e){status(e.message,true);}};
const invitation=location.hash.match(/^#invite=([0-9a-f]{64})$/);if(invitation){pendingToken=invitation[1];history.replaceState(null,'',location.pathname);$('welcome').hidden=false;$('enrol').hidden=false;$('welcomeText').textContent='Denne private invitasjonen kobler enheten til familiens bildeskjerm og besøk.';}
$('enrol').onclick=async()=>{if(!pendingToken)return;try{const a=await api('enrol',{token:pendingToken});pendingToken=null;$('enrol').hidden=true;if(a.kind==='auto')history.replaceState(null,'','/fruen/klargjor');await boot();}catch(e){$('welcomeText').textContent=e.message;}};
if(!invitation)boot();
setInterval(loadPhotos,60000);
setInterval(()=>{
 if(call&&Date.now()-lastPoll>10000){status('Forbindelsen ble brutt. Kamera og mikrofon er slått av.',true);connectionNotice=true;endCall('connection');}
 // iOS can suspend or interrupt audio; keep trying quietly. The name is always shown.
 if(armed&&context&&context.state!=='running')context.resume().catch(()=>{});
},1000);
document.addEventListener('visibilitychange',()=>{if(document.hidden){endCall('hidden');api('state',{ready:false,after:0}).catch(()=>{});wake?.release().catch(()=>{});wake=null;}else{requestWake();poll();}});
window.addEventListener('pagehide',()=>{const id=call?.id;stopCall();armed=false;if(id)fetch('/fruen/api/call/'+id+'/end',{method:'POST',headers:{'Content-Type':'application/json','X-Fruen-Request':'1'},body:'{"reason":"hidden"}',keepalive:true}).catch(()=>{});});
})();
