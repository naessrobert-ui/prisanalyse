'use strict';
// Keeps the frame page itself available while the Pi is down or restarting. If iPad Safari
// reloads the tab during an outage, it gets the cached page, which keeps trying on its own,
// instead of an error page that needs someone on site to tap reload.
// Only the page and app.js are cached: no photos, names or other private data.
const CACHE='fruen-shell-v1';
const PAGE='/fruen/',APP='/fruen/app.js';
self.addEventListener('install',event=>{event.waitUntil(caches.open(CACHE).then(c=>c.addAll([PAGE,APP])).catch(()=>{}));self.skipWaiting();});
self.addEventListener('activate',event=>{event.waitUntil((async()=>{for(const key of await caches.keys())if(key.startsWith('fruen-')&&key!==CACHE)await caches.delete(key);await self.clients.claim();})());});
self.addEventListener('fetch',event=>{
 const request=event.request,url=new URL(request.url);
 if(request.method!=='GET'||url.origin!==self.location.origin)return;
 const key=request.mode==='navigate'&&['/fruen','/fruen/','/fruen/klargjor'].includes(url.pathname)?PAGE:url.pathname===APP?APP:null;
 if(!key)return;  // API, photos, admin: always straight to the network
 event.respondWith((async()=>{
  try{
   const response=await fetch(request);
   if(response.ok){const copy=response.clone();event.waitUntil(caches.open(CACHE).then(c=>c.put(key,copy)).catch(()=>{}));return response;}
   return (await caches.match(key))||response;  // e.g. 503 from the proxy while the Pi is down
  }catch(error){const hit=await caches.match(key);if(hit)return hit;throw error;}
 })());
});
