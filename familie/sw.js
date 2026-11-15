'use strict';
self.addEventListener('push',event=>{
 let data={};try{data=event.data.json();}catch(e){}
 const count=Number.isInteger(data.badge)&&data.badge>0?data.badge:0;
 const url=typeof data.url==='string'&&data.url.startsWith('/familie/')?data.url:'/familie/';
 event.waitUntil(Promise.all([
  self.registration.showNotification(typeof data.title==='string'&&data.title?data.title.slice(0,120):'Nytt i familierommet',{
   body:typeof data.body==='string'&&data.body?data.body.slice(0,200):'Familien har delt noe nytt. Åpne familierommet for å se det.',
   tag:'family-new',renotify:true,icon:'/familie/icon.png',badge:'/familie/icon.png',data:{url}
  }),
  count&&self.navigator.setAppBadge?self.navigator.setAppBadge(count).catch(()=>{}):Promise.resolve()
 ]));
});
self.addEventListener('notificationclick',event=>{
 event.notification.close();
 const url=event.notification.data?.url||'/familie/';
 event.waitUntil((async()=>{const windows=await clients.matchAll({type:'window',includeUncontrolled:true});for(const client of windows){if(new URL(client.url).pathname.startsWith('/familie')){await client.focus();await client.navigate(url);return;}}await clients.openWindow(url);})());
});
