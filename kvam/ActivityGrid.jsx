const ALL_ACTIVITIES = [
  // VÅR
  { id:'a1', season:'spring', seasonLabel:'Vår', photo:'photo-lavlands', kicker:'Lavlandsløype · 10 km', title:'Lavlandsløypen rundt', distance:'10 km', difficulty:'Lett', duration:'2–3 t' },
  { id:'a2', season:'spring', seasonLabel:'Vårski', photo:'photo-vaarski', kicker:'Vårski · mot Folgefonna', title:'Topptur i påska', distance:'7,4 km', difficulty:'Middels', duration:'4 t' },
  { id:'a3', season:'spring', seasonLabel:'Vår', photo:'photo-grusvei', kicker:'Sykkeltur · grusveg', title:'Mødalen rundt', distance:'18 km', difficulty:'Middels', duration:'3 t' },
  // VINTER
  { id:'a4', season:'winter', seasonLabel:'Ski', photo:'photo-ski-fure', kicker:'Skiløype · 8,4 km', title:'Furedalen → Mødal', distance:'8,4 km', difficulty:'Middels', duration:'2–3 t' },
  { id:'a5', season:'winter', seasonLabel:'Topptur', photo:'photo-tveita', kicker:'Topptur · 1 299 moh.', title:'Tveitakvitingen', distance:'5,8 km', difficulty:'Krevende', duration:'4–5 t' },
  { id:'a6', season:'winter', seasonLabel:'Topptur', photo:'photo-saata', kicker:'Skitur · sol og skugge', title:'Såta rundt', distance:'12 km', difficulty:'Middels', duration:'3–4 t' },
  // SOMMER
  { id:'a7', season:'summer', seasonLabel:'Sommer', photo:'photo-saata-s', kicker:'Fjelltur · 7,2 km', title:'Mot Såta i sommarvêr', distance:'7,2 km', difficulty:'Lett', duration:'2–3 t' },
  { id:'a8', season:'summer', seasonLabel:'Sommer', photo:'photo-isflak', kicker:'Bading · 1 100 moh.', title:'Isflak i fjellvatnet', distance:'—', difficulty:'Familie', duration:'—' },
  { id:'a9', season:'summer', seasonLabel:'Sommer', photo:'photo-fjord', kicker:'Utsiktstur · Hardangerfjorden', title:'Mot Folgefonna og fjorden', distance:'9,4 km', difficulty:'Krevende', duration:'5–6 t' },
];

const SEASONS = [
  { id:'all',    label:'Alle' },
  { id:'spring', label:'Vår',    eb:'spring' },
  { id:'summer', label:'Sommer', eb:'summer' },
  { id:'autumn', label:'Høst',   eb:'autumn' },
  { id:'winter', label:'Vinter', eb:'winter' },
];

const ActivityGrid = ({ defaultSeason = 'spring' }) => {
  const [season, setSeason] = React.useState(defaultSeason);
  const items = season === 'all' ? ALL_ACTIVITIES : ALL_ACTIVITIES.filter(a => a.season === season);
  const ebClass = SEASONS.find(s => s.id === season)?.eb || 'spring';
  return (
    <section className="section">
      <div className="container">
        <div className="section-head">
          <div className="titles">
            <div className={"eyebrow " + ebClass}><span className="dot"/>Aktiviteter · uke 18</div>
            <h2>Hva skal du gjøre i helgen?</h2>
            <p className="lede">Lavlandet blomstrar og gjellane har framleis snø — vel sesong, så syner vi det som er ope no.</p>
          </div>
          <button className="btn-ghost">Sjå alle aktivitetar →</button>
        </div>
        <div className="season-filter">
          {SEASONS.map(s => (
            <button key={s.id}
              className={"chip" + (season === s.id ? " active" : "")}
              onClick={() => setSeason(s.id)}>
              {s.label}
            </button>
          ))}
        </div>
        <div className="activity-grid">
          {items.map(a => <ActivityCard key={a.id} a={a}/>)}
        </div>
      </div>
    </section>
  );
};
window.ActivityGrid = ActivityGrid;
