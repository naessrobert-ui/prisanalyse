// Inline SVG Lucide subset + custom spruce mark.
// Stroke 1.5, currentColor.

const Icon = ({ name, size = 18, ...rest }) => {
  const props = {
    width: size, height: size, viewBox: "0 0 24 24",
    fill: "none", stroke: "currentColor", strokeWidth: 1.5,
    strokeLinecap: "round", strokeLinejoin: "round",
    ...rest,
  };
  switch (name) {
    case "sun":
      return <svg {...props}><circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32 1.41 1.41M2 12h2m16 0h2M4.93 19.07l1.41-1.41m11.32-11.32 1.41-1.41"/></svg>;
    case "cloud":
      return <svg {...props}><path d="M17.5 19a4.5 4.5 0 1 0 0-9 5.5 5.5 0 0 0-10.7 1A4 4 0 0 0 7 19h10.5z"/></svg>;
    case "cloud-snow":
      return <svg {...props}><path d="M17.5 14a4.5 4.5 0 1 0 0-9 5.5 5.5 0 0 0-10.7 1A4 4 0 0 0 7 14h10.5z"/><path d="M8 19v.01M8 16v.01M12 21v.01M12 18v.01M16 19v.01M16 16v.01"/></svg>;
    case "wind":
      return <svg {...props}><path d="M17.7 7.7a2.5 2.5 0 1 1 1.8 4.3H2M9.6 4.6A2 2 0 1 1 11 8H2m10.6 11.4A2 2 0 1 0 14 16H2"/></svg>;
    case "thermometer":
      return <svg {...props}><path d="M14 4v10.5a4 4 0 1 1-4 0V4a2 2 0 1 1 4 0z"/></svg>;
    case "map-pin":
      return <svg {...props}><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 1 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>;
    case "map":
      return <svg {...props}><path d="m3 6 6-3 6 3 6-3v15l-6 3-6-3-6 3z"/><path d="M9 3v15M15 6v15"/></svg>;
    case "mountain":
      return <svg {...props}><path d="m8 3 4 8 5-5 5 15H2L8 3z"/></svg>;
    case "snowflake":
      return <svg {...props}><path d="M2 12h20M12 2v20M4.93 4.93l14.14 14.14M19.07 4.93 4.93 19.07"/></svg>;
    case "compass":
      return <svg {...props}><circle cx="12" cy="12" r="10"/><polygon points="16.24 7.76 14.12 14.12 7.76 16.24 9.88 9.88"/></svg>;
    case "arrow-right":
      return <svg {...props}><path d="M5 12h14M12 5l7 7-7 7"/></svg>;
    case "plus":
      return <svg {...props}><path d="M12 5v14M5 12h14"/></svg>;
    case "x":
      return <svg {...props}><path d="M18 6 6 18M6 6l12 12"/></svg>;
    case "check":
      return <svg {...props} strokeWidth={2}><path d="M20 6 9 17l-5-5"/></svg>;
    case "calendar":
      return <svg {...props}><rect x="3" y="4" width="18" height="18" rx="2"/><path d="M16 2v4M8 2v4M3 10h18"/></svg>;
    case "clock":
      return <svg {...props}><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg>;
    case "heart":
      return <svg {...props}><path d="M19 14c1.5-1.5 3-3.3 3-5.5A5.5 5.5 0 0 0 16.5 3c-1.8 0-3 .5-4.5 2-1.5-1.5-2.7-2-4.5-2A5.5 5.5 0 0 0 2 8.5c0 2.2 1.5 4 3 5.5l7 7Z"/></svg>;
    default:
      return null;
  }
};

window.Icon = Icon;
