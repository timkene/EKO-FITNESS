/**
 * Jersey-style avatar: short name on top, number below (like a mini jersey).
 * Use instead of profile pictures for players.
 * @param {string} shortName - First name or baller name (short, e.g. "Alex")
 * @param {number|string} number - Jersey number
 * @param {string} [className] - Extra classes for the wrapper
 * @param {string} [status] - 'in' | 'out' | 'maybe' for status dot
 */
export default function JerseyAvatar({ shortName, number, className = '', status, size }) {
  const displayName = (shortName || '')
    .trim()
    .split(/\s+/)[0]
    .slice(0, 8) || '—';
  const num = number != null ? String(number) : '?';
  const isLarge = size === 'lg';

  const statusDot = {
    in: 'bg-primary border-2 border-background-dark',
    out: 'bg-slate-600 border-2 border-background-dark',
    maybe: 'bg-amber-500 border-2 border-background-dark',
  }[status];

  return (
    <div className={`relative inline-flex ${className}`}>
      <div
        className={`rounded-xl bg-primary flex flex-col items-center justify-center text-background-dark overflow-hidden ${isLarge ? 'min-w-[5rem] min-h-[5rem] w-24 h-24 md:w-28 md:h-28' : 'min-w-[2.5rem] min-h-[2.5rem] w-10 h-10'}`}
        aria-hidden
      >
        <span className={isLarge ? 'text-xs font-bold leading-none uppercase' : 'text-[8px] font-bold leading-none uppercase'}>{displayName}</span>
        <span className={isLarge ? 'text-3xl md:text-4xl font-black leading-none' : 'text-lg font-black leading-none'}>{num}</span>
      </div>
      {statusDot && (
        <span
          className={`absolute bottom-0 right-0 size-3 rounded-full ${statusDot}`}
          aria-hidden
        />
      )}
    </div>
  );
}
