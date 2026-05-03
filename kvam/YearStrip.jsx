const YEAR_TILES = [
  { cls:'tile-vinter', eb:'Desember–April', nm:'Skiløyper og toppturar' },
  { cls:'tile-vaar',   eb:'April–Mai',      nm:'Vårski mot Folgefonna' },
  { cls:'tile-sommer', eb:'Juni–August',    nm:'Bading i fjellvatn' },
  { cls:'tile-haust',  eb:'September–Oktober', nm:'Klar luft, roleg vatn' },
];

const YearStrip = () => (
  <section className="section tight">
    <div className="container">
      <div className="section-head">
        <div className="titles">
          <div className="eyebrow summer"><span className="dot"/>Året rundt</div>
          <h2>Same staden, fire forteljingar.</h2>
          <p className="lede">Kvamskogen er ikkje éin sesong. Det er fire — og kvar av dei har sine eigne løyper, sin eigen lyd, sitt eige lys.</p>
        </div>
      </div>
      <div className="year-strip">
        {YEAR_TILES.map((t,i) => (
          <div key={i} className={"year-tile " + t.cls}>
            <div className="label">
              <div className="eb">{t.eb}</div>
              <div className="nm">{t.nm}</div>
            </div>
          </div>
        ))}
      </div>
    </div>
  </section>
);

const MoodBlock = () => (
  <section className="section tight">
    <div className="container mood-block">
      <div className="mood-photo" style={{backgroundImage:"url('assets/photos/furedalen-snofall-natt.jpg')"}}>
        <div className="cap"><small>Furedalen, januar</small>Det snør ofte. Nokre år meir enn andre — og nokre år blir hyttene heilt nedsnødde.</div>
      </div>
      <div className="mood-photo" style={{backgroundImage:"url('assets/photos/loypemaskin-natt.jpg')"}}>
        <div className="cap"><small>Løypene · natt til søndag</small>Maskinene kjøyrer om natta, slik at det er klart til søndagsturen.</div>
      </div>
    </div>
  </section>
);

window.YearStrip = YearStrip;
window.MoodBlock = MoodBlock;
