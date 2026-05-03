const Spruce = ({ size = 24, ...rest }) => (
  <svg width={size} height={size} viewBox="0 0 32 32" fill="currentColor" aria-hidden="true" {...rest}>
    <path d="M16 2.4c-.4 0-.7.2-.9.6L11.7 9c-.3.6.1 1.3.8 1.3h.7l-3.5 5.7c-.4.6 0 1.4.7 1.4h1.2L7.8 23c-.5.6 0 1.5.8 1.5h5.6v3.4c0 .9.7 1.6 1.6 1.6h.5c.9 0 1.6-.7 1.6-1.6v-3.4h5.5c.7 0 1.2-.9.7-1.5l-3.7-5.6h1.2c.7 0 1.1-.8.7-1.4l-3.4-5.7h.7c.7 0 1.1-.7.8-1.3L17 3c-.2-.4-.5-.6-.9-.6h-.1z"/>
  </svg>
);
const Wordmark = ({ onClick }) => (
  <a className="kk-brand" onClick={onClick} style={{cursor:'pointer'}}>
    <Spruce size={26} style={{color: 'var(--granskog-700)'}}/>
    Kvamskogen
  </a>
);
window.Spruce = Spruce;
window.Wordmark = Wordmark;
