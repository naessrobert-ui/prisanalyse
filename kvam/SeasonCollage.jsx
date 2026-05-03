// To kollasje-seksjoner: én for vinter, én for sommer.
// Bruker bilder som ellers ikke vises på siden.

const WINTER_PHOTOS = [
  { src: 'assets/photos/hytte-snodd-ned.jpg',         cap: 'Hytte snødd ned' },
  { src: 'assets/photos/modalen-djup-sno.jpg',        cap: 'Djup snø i Modalen' },
  { src: 'assets/photos/modalen-vinter.jpg',          cap: 'Modalen, januar' },
  { src: 'assets/photos/saata-nysno-toppturarar.jpg', cap: 'Sååta · nysnø og toppturfolk' },
  { src: 'assets/photos/topptur-1300-skylag.jpg',     cap: 'Topptur 1300 moh.' },
  { src: 'assets/photos/tveitakvitingen-skispor.jpg', cap: 'Tveitakvitingen · skispor' },
  { src: 'assets/photos/utsikt-vinter.jpg',           cap: 'Utsikt · vinter' },
  { src: 'assets/photos/furedalen-snofall-natt.jpg',  cap: 'Furedalen · snøfall om natta' },
];

const SUMMER_PHOTOS = [
  { src: 'assets/photos/tveitakvitingen-sommar.jpg', cap: 'Tveitakvitingen · sommar' },
  { src: 'assets/photos/saata-sommar.jpg',           cap: 'Sååta i juli' },
  { src: 'assets/photos/sommar-fjell-isflak.jpg',    cap: 'Fjellvatn · isflak' },
  { src: 'assets/photos/hardangerfjorden.jpg',       cap: 'Hardangerfjorden' },
  { src: 'assets/photos/grusvei-stol.jpg',           cap: 'Grusvei · stol' },
  { src: 'assets/photos/utsikt-fjord.jpg',           cap: 'Utsikt mot fjorden' },
];

const Collage = ({ photos, eyebrowClass, eyebrow, title, lede }) => (
  <section className="section" id={eyebrowClass==='winter' ? 'vinter' : (eyebrowClass==='summer' ? 'sommer' : undefined)}>
    <div className="container">
      <div className={"eyebrow " + eyebrowClass}><span className="dot"/>{eyebrow}</div>
      <h2 style={{fontFamily:'var(--font-display)', fontSize:'clamp(34px,4.5vw,56px)', fontWeight:500, lineHeight:1.05, letterSpacing:'-0.02em', margin:'0 0 14px', maxWidth:780}}>{title}</h2>
      <p className="lede" style={{marginBottom:32, maxWidth:680}}>{lede}</p>
      <div className="collage-grid">
        {photos.map((p, i) => (
          <figure key={i} className={"collage-tile collage-tile-" + (i % 6)}
            style={{backgroundImage:`url('${p.src}')`}}>
            <figcaption>{p.cap}</figcaption>
          </figure>
        ))}
      </div>
    </div>
  </section>
);

const WinterCollage = () => (
  <Collage
    photos={WINTER_PHOTOS}
    eyebrowClass="winter"
    eyebrow="Kvamskogen · vinter"
    title="Når snøen kjem, blir det stille."
    lede="Frå dei første snøfalla i november til vårski på Folgefonna i mai. Mørke morgonar, lyse netter under løypelyset, og toppturar når sola endeleg snur."
  />
);

const SummerCollage = () => (
  <Collage
    photos={SUMMER_PHOTOS}
    eyebrowClass="summer"
    eyebrow="Kvamskogen · sommer"
    title="Sommaren er kort, men lang nok."
    lede="Bading i fjellvatn, toppturar utan stavar, og fjorden eit kvarter unna. Mellom juni og august er Kvamskogen ein annan stad."
  />
);

window.WinterCollage = WinterCollage;
window.SummerCollage = SummerCollage;
