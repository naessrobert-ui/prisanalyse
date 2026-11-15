(()=>{'use strict';
const $=id=>document.getElementById(id),key='motes-plans-v1';let base='',setMode=null,editing=null,shown=null,owner=null,invite=null,available=false,busy=false;
const validRoom=r=>/^plan-[0-9a-f]{20}$/.test(r||'');
function registry(){try{const v=JSON.parse(localStorage.getItem(key)||'[]');return Array.isArray(v)?v.filter(x=>validRoom(x.room)&&/^[0-9a-f]{64}$/.test(x.owner)).slice(0,40):[];}catch(e){return [];}}
function remember(plan,token){const list=registry().filter(p=>p.room!==plan.room);list.unshift({room:plan.room,owner:token,title:plan.title,start:plan.start,timezone:plan.timezone,cancelled:plan.cancelled});try{localStorage.setItem(key,JSON.stringify(list.slice(0,40)));}catch(e){$('planStatus').textContent='Nettleseren kunne ikke huske møtet. Kopier den private arrangørlenken før du lukker siden.';}}
async function request(path,options={}){const r=await fetch(base+'/meet/plans'+path,{...options,headers:{'Content-Type':'application/json','X-Meet-Request':'1',...(options.headers||{})}});if(!r.ok){let e;try{e=await r.json();}catch(_){}throw Error(e?.detail||'Møtet kunne ikke hentes.');}return r;}
const headers=()=>owner?{'X-Plan-Owner':owner}:{'X-Plan-Invite':invite};
function when(plan){return new Intl.DateTimeFormat('nb-NO',{dateStyle:'full',timeStyle:'short',timeZone:plan.timezone}).format(new Date(plan.start))+' · '+plan.duration+' min · '+plan.timezone;}
function localValue(date,zone){const parts=new Intl.DateTimeFormat('sv-SE',{timeZone:zone,year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(date);const get=t=>parts.find(x=>x.type===t)?.value;return get('year')+'-'+get('month')+'-'+get('day')+'T'+get('hour')+':'+get('minute');}
function defaultTime(){const d=new Date(Date.now()+3600000);d.setMinutes(Math.ceil(d.getMinutes()/15)*15,0,0);$('planStart').value=localValue(d,$('planZone').value);}
function message(text){$('planStatus').textContent=text;}
function render(plan,token=null,guest=null){
 shown=plan;owner=token;invite=guest||plan.inv;message('');$('planTitleOut').textContent=plan.title;$('planWhenOut').textContent=when(plan);$('planAgendaOut').textContent=plan.agenda||'Ingen agenda lagt inn.';$('planJoinUrl').value=plan.join_url;
 $('planCancelled').classList.toggle('hidden',!plan.cancelled);$('planJoinNow').disabled=plan.cancelled||new Date(plan.expires)<new Date();$('planEdit').classList.toggle('hidden',!owner||plan.cancelled);$('planCancel').classList.toggle('hidden',!owner||plan.cancelled);$('planOwner').classList.toggle('hidden',!owner);
 $('planCalendar').textContent=plan.cancelled?'Last ned avlysning til kalender':'Last ned kalenderinvitasjon';$('planOwnerUrl').value=owner?location.origin+location.pathname+'#plan-eier='+plan.room+'.'+owner:'';
 if(!$('planResult').open)$('planResult').showModal();
}
async function loadOwner(room,token){const r=await request('/'+room,{headers:{'X-Plan-Owner':token}});const plan=await r.json();render(plan,token);remember(plan,token);return plan;}
function mode(value){$('scheduleFields').classList.toggle('hidden',value!=='plan');if(value==='plan'){$('joinBtn').disabled=!available||busy;$('joinBtn').textContent=editing?'Lagre endringer':'Lagre planlagt møte';$('landNote').textContent=available?'Møtet starter ikke nå. Du får en lenke og kalenderfil.':'Sjekker privat lagring for planlagte møter …';}}
async function save(){
 if(busy)return;if(!available){$('landNote').textContent='Planlegging er midlertidig utilgjengelig. Du kan fortsatt opprette et møte nå.';return;}
 const data={title:$('planTitle').value,name:$('nameIn').value,local_start:$('planStart').value,timezone:$('planZone').value,duration:Number($('planDuration').value),agenda:$('planAgenda').value};
 busy=true;for(const id of ['createMode','planMode','joinMode'])$(id).disabled=true;$('joinBtn').disabled=true;$('landNote').textContent='Lagrer møtet …';
 try{
  const prior=editing;const r=await request(prior?'/'+prior.room:'',{method:prior?'PUT':'POST',headers:prior?{'X-Plan-Owner':prior.owner}:{},body:JSON.stringify({...data,...(prior?{version:prior.version}:{})})});const p=await r.json(),token=prior?prior.owner:p.owner;
  editing={room:p.room,owner:token,version:p.version};render(p,token);remember(p,token);$('landNote').textContent=prior?'Møtet er oppdatert. Del en ny kalenderfil med deltakerne.':'Møtet er planlagt. Del invitasjonen med deltakerne.';
  try{localStorage.setItem('nammeet-name',data.name);}catch(e){}
 }catch(e){$('landNote').textContent=e.message;}
 finally{busy=false;for(const id of ['createMode','planMode','joinMode'])$(id).disabled=false;$('joinBtn').disabled=false;}
}
function fill(plan,token){
 editing={room:plan.room,owner:token,version:plan.version};$('nameIn').value=plan.name;$('planTitle').value=plan.title;if(!Array.from($('planZone').options).some(x=>x.value===plan.timezone))$('planZone').add(new Option(plan.timezone,plan.timezone));$('planZone').value=plan.timezone;$('planStart').value=localValue(new Date(plan.start),plan.timezone);
 if(!Array.from($('planDuration').options).some(x=>x.value===String(plan.duration)))$('planDuration').add(new Option(plan.duration+' minutter',String(plan.duration)));$('planDuration').value=String(plan.duration);$('planAgenda').value=plan.agenda;setMode('plan');$('planResult').close();$('planTitle').focus();
}
async function mine(){
 $('plansList').replaceChildren();if(!$('plansDialog').open)$('plansDialog').showModal();const items=registry();if(!items.length){$('plansListNote').textContent='Ingen møter er lagret i denne nettleseren. Åpne en privat arrangørlenke for å hente et møte fra en annen enhet.';return;}
 $('plansListNote').textContent='Disse møtene er husket i denne nettleseren. Arrangørlenken kan brukes på andre enheter.';
 for(const item of items){const b=document.createElement('button');b.className='sbtn plan-list-item';b.textContent=(item.title||'Planlagt møte')+(item.start?' · '+new Intl.DateTimeFormat('nb-NO',{dateStyle:'medium',timeStyle:'short',timeZone:item.timezone||'Europe/Oslo'}).format(new Date(item.start)):'')+(item.cancelled?' · avlyst':'');b.onclick=async()=>{try{await loadOwner(item.room,item.owner);$('plansDialog').close();}catch(e){$('plansListNote').textContent=e.message;}};$('plansList').append(b);}
}
async function copy(text,notice){try{await navigator.clipboard.writeText(text);message(notice);}catch(e){message('Kunne ikke kopiere. Marker lenken i feltet og kopier den.');}}
$('planCopy').onclick=()=>copy(shown.join_url,'Møtelenken er kopiert.');$('planCopyOwner').onclick=()=>copy($('planOwnerUrl').value,'Den private arrangørlenken er kopiert. Den gir rett til å endre og avlyse møtet.');
$('planShare').onclick=async()=>{if(!shown)return;const text=(shown.cancelled?'AVLYST: ':'')+shown.title+'\n'+when(shown)+'\n'+(shown.agenda?shown.agenda+'\n':'')+'\nBli med: '+shown.join_url;try{if(navigator.share)await navigator.share({title:shown.title,text,url:shown.join_url});else await copy(text,'Invitasjonen er kopiert. Lim den inn i e-post eller en melding.');}catch(e){if(e.name!=='AbortError')message('Kunne ikke dele. Bruk «Kopier møtelenke».');}};
$('planCalendar').onclick=async()=>{try{const r=await request('/'+shown.room+'/calendar',{method:'POST',headers:headers(),body:'{}'});const url=URL.createObjectURL(await r.blob());const a=document.createElement('a');a.href=url;a.download=shown.room+'.ics';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);message('Åpne kalenderfilen i Outlook, Google Kalender eller Apple Kalender. Ved endringer må du dele en oppdatert fil.');}catch(e){message(e.message);}};
$('planEdit').onclick=()=>fill(shown,owner);
$('planCancel').onclick=async()=>{if(!confirm('Avlyse møtet? Nye deltakere slipper ikke inn med lenken. Du må selv informere deltakerne og dele avlysningen til kalenderen.'))return;try{const r=await request('/'+shown.room+'/cancel',{method:'POST',headers:headers(),body:'{}'});render(await r.json(),owner);remember(shown,owner);message('Møtet er avlyst. Del en avlysning eller beskjed med deltakerne.');}catch(e){message(e.message);}};
$('planDelete').onclick=async()=>{if(!owner||!confirm('Slette møtedetaljene og gjøre lenken ugyldig? Dette oppdaterer ikke deltakernes kalendere. Avlys og del en avlysning først hvis andre er invitert.'))return;try{await request('/'+shown.room,{method:'DELETE',headers:headers(),body:'{}'});try{localStorage.setItem(key,JSON.stringify(registry().filter(p=>p.room!==shown.room)));}catch(e){}$('planResult').close();$('landNote').textContent='Møtet er slettet.';}catch(e){message(e.message);}};
$('planJoinNow').onclick=()=>{if(shown&&!shown.cancelled){$('planResult').close();location.hash=shown.room+'?inv='+shown.inv;$('roomIn').value=shown.room+'?inv='+shown.inv;setMode('join');$('nameIn').focus();}};
$('planClose').onclick=()=>$('planResult').close();$('plansClose').onclick=()=>$('plansDialog').close();$('myPlans').onclick=mine;
window.MeetPlanner={mode,save,async guest(room,token){const r=await request('/'+room,{headers:{'X-Plan-Invite':token}});return r.json();},showGuest(plan,token){render(plan,null,token);},init(options){
 base=options.base;setMode=options.setMode;if(!options.publicSite){$('planMode').classList.add('hidden');$('myPlans').classList.add('hidden');return;}
 const browserZone=Intl.DateTimeFormat().resolvedOptions().timeZone;if(browserZone&&!['Europe/Oslo','UTC'].includes(browserZone))$('planZone').add(new Option(browserZone,browserZone));defaultTime();
 $('planMode').onclick=()=>{editing=null;setMode('plan');};
 request('/status').then(r=>r.json()).then(v=>{available=!!v.available;if($('planMode').getAttribute('aria-pressed')==='true'){mode('plan');if(!available)$('landNote').textContent=v.detail||'Planlegging er midlertidig utilgjengelig.';}}).catch(()=>{available=false;});
 const link=location.hash.match(/^#plan-eier=(plan-[0-9a-f]{20})\.([0-9a-f]{64})$/);if(link)loadOwner(link[1],link[2]).catch(e=>{$('landNote').textContent=e.message;});
}};
})();
