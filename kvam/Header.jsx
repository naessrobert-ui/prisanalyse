const Header = ({ overHero, onNav, route, onAdd }) => {
  const scrollToId = (id) => {
    if (route !== 'home') { onNav('home'); setTimeout(() => { const el = document.getElementById(id); if (el) el.scrollIntoView({behavior:'smooth', block:'start'}); }, 60); }
    else { const el = document.getElementById(id); if (el) el.scrollIntoView({behavior:'smooth', block:'start'}); }
  };
  return (
    <header className={"kk-header" + (overHero ? " over-hero" : "")}>
      <Wordmark onClick={() => onNav('home')}/>
      <nav className="kk-nav">
        <a className={route==='weather'?'active':''} onClick={() => onNav('weather')}>Vêr</a>
        <a onClick={() => scrollToId('vinter')}>Vinter</a>
        <a onClick={() => scrollToId('sommer')}>Sommer</a>
        <a href="/ver/skiloyper-kvamskogen" target="_blank" rel="noopener">Løyper – vinter ↗</a>
        <a href="https://www.kvamskogen-vel.no/wp-content/uploads/2025/08/Lavlandsloypen-over-Kvamskogen-pr-juli-2025-4.pdf" target="_blank" rel="noopener">Lavlandsløypen ↗</a>
        <a onClick={() => onNav('about')}>Om</a>
        <a href="https://www.kvamskogen-vel.no/" target="_blank" rel="noopener">Kvamskogen Vel ↗</a>
      </nav>
      <div className="spacer"/>
      <button className="btn btn-accent btn-sm" onClick={onAdd}>
        <Icon name="plus" size={14} style={{marginRight:6, verticalAlign:-2}}/>
        Legg til aktivitet
      </button>
    </header>
  );
};
window.Header = Header;
