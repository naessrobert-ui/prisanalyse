'use strict';
const form=document.querySelector('#mobile-form'), button=document.querySelector('#run'), statusBox=document.querySelector('#status');
const base='/bil/import-radar';
let active=null, started=0;
function status(text,error=false){statusBox.textContent=text;statusBox.className=error?'error':active?'busy':'';}
function render(report,id){
  if(!report || report.report_type!=='mobile_export')return;
  document.querySelector('#results').hidden=false;
  document.querySelector('#result-title').textContent=`${report.biler.length} biler hentet`;
  document.querySelector('#scope').textContent=`${report.sok_sider} søkesider · ${report.sok_status}`;
  for(const kind of ['xlsx','csv','json'])document.querySelector('#'+kind).href=`${base}/api/mobile/${encodeURIComponent(id)}/download/${kind}`;
  const warnings=document.querySelector('#warnings');warnings.replaceChildren();
  for(const message of [...(report.sok_merknader||[]),...(report.feil||[])]){const li=document.createElement('li');li.textContent=String(message);warnings.append(li);}
  const body=document.querySelector('#rows');body.replaceChildren();
  for(const car of report.biler){
    const row=document.createElement('tr'),cell=document.createElement('td');
    let link;try{const u=new URL(car.url);if(u.protocol==='https:'&&u.hostname==='suchen.mobile.de')link=u.href;}catch{}
    const name=document.createElement(link?'a':'span');name.textContent=car.tittel||car.annonse_id;
    if(link){name.href=link;name.target='_blank';name.rel='noopener noreferrer';}cell.append(name);row.append(cell);
    for(const key of ['pris_eur','nettopris_eur','km','forstegangsregistrering','batteri_kwh','rekkevidde_wltp_km','egenvekt_kg','selger','status']){
      const td=document.createElement('td'),v=car[key];td.textContent=v==null||v===''?'—':typeof v==='number'?v.toLocaleString('nb-NO'):String(v);if(key==='status')td.title=car.merknad||'';row.append(td);
    }body.append(row);
  }
}
async function responseJSON(response){if(!response.headers.get('content-type')?.includes('application/json'))throw Error('Økten kan ha utløpt. Logg inn på nytt og åpne denne siden.');const data=await response.json();if(!response.ok)throw Error(data.error||'Forespørselen feilet');return data;}
async function poll(){
  const id=active;if(!id)return;
  try{
    const job=await responseJSON(await fetch(`${base}/api/search/${encodeURIComponent(id)}`,{cache:'no-store'}));
    render(job.report,id);
    if(job.status==='running'){
      if(Date.now()-started>270000)throw Error('Innhentingen bruker for lang tid. Hentede rader kan lastes ned ovenfor.');
      status(`Henter bildata … ${job.report?.biler?.length||0} biler lest.`);setTimeout(poll,2000);return;
    }
    active=null;button.disabled=false;
    const failed=job.status==='error'||job.report?.kjorestatus==='feil';
    status(job.error||(failed?job.report.sok_status:job.report?.kjorestatus==='delvis'?'Innhentingen stoppet underveis. Du kan laste ned bilene som ble hentet.':'Uttrekket er klart. Kontroller søkeomfanget og last ned filen.'),failed);
  }catch(error){active=null;button.disabled=false;status(error.message,true);}
}
form.addEventListener('submit',async event=>{
  event.preventDefault();if(active)return;
  button.disabled=true;document.querySelector('#results').hidden=true;status('Starter innhenting …');
  try{
    const data=new FormData(form),job=await responseJSON(await fetch(`${base}/api/mobile`,{method:'POST',headers:{'Content-Type':'application/json','X-CSRF-Token':form.dataset.csrf},body:JSON.stringify({url:data.get('url'),limit:Number(data.get('limit'))})}));
    active=job.id;started=Date.now();poll();
  }catch(error){button.disabled=false;status(error.message,true);}
});
