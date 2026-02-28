import { useEffect, useState } from 'react';
import { Outlet, NavLink, useNavigate, Link } from 'react-router-dom';
import { getPlayerAuth, clearPlayerAuth } from '../pages/Login';
import { getMemberStats } from '../api';
import JerseyAvatar from './JerseyAvatar';

function useDarkMode() {
  const [dark, setDark] = useState(() => {
    const saved = localStorage.getItem('eko_dark_mode');
    if (saved !== null) return saved === 'true';
    return window.matchMedia('(prefers-color-scheme: dark)').matches;
  });

  useEffect(() => {
    document.documentElement.classList.toggle('dark', dark);
    localStorage.setItem('eko_dark_mode', String(dark));
  }, [dark]);

  return [dark, setDark];
}

export default function MemberLayout() {
  const navigate = useNavigate();
  const { token, player } = getPlayerAuth();
  const [menuOpen, setMenuOpen] = useState(false);
  const [starRating, setStarRating] = useState(null);
  const [dark, setDark] = useDarkMode();

  useEffect(() => {
    if (!token || !player) navigate('/login', { replace: true });
  }, [token, player, navigate]);

  useEffect(() => {
    if (!token) return;
    getMemberStats(token)
      .then((d) => setStarRating(d?.star_rating ?? null))
      .catch(() => setStarRating(null));
  }, [token]);

  const handleLogout = () => {
    clearPlayerAuth();
    navigate('/login', { replace: true });
  };

  const closeMenu = () => setMenuOpen(false);

  if (!token || !player) return null;

  const navClass = "flex items-center gap-3 px-3 py-3 min-h-[44px] rounded-lg text-slate-400 hover:bg-primary/5 hover:text-primary transition-colors touch-manipulation";
  const navActive = "bg-primary/10 text-primary font-semibold";

  const SidebarContent = () => (
    <>
      <div className="p-4 md:p-6 flex items-center gap-3">
        <div className="bg-primary size-10 rounded flex items-center justify-center text-background-dark shrink-0">
          <span className="material-symbols-outlined font-bold text-2xl">sports_soccer</span>
        </div>
        <div className="min-w-0">
          <h1 className="text-lg font-bold leading-tight">Eko Football</h1>
          <p className="text-xs text-primary font-medium uppercase tracking-wider">Player Portal</p>
        </div>
      </div>
      <div className="px-3 py-2.5 md:px-4 md:py-3 mx-2 rounded-xl bg-amber-500/10 border border-amber-500/20 mt-2 min-h-[44px] flex flex-col justify-center touch-manipulation">
        {typeof starRating === 'number' && starRating >= 1 ? (
          <>
            <p className="text-[10px] md:text-xs font-bold text-amber-500 uppercase tracking-wider mb-0.5">You are a</p>
            <p className="text-base md:text-lg font-black text-amber-400 flex items-center gap-1 flex-wrap leading-tight">
              <span className="inline-flex text-sm md:text-base" aria-hidden>{'★'.repeat(Math.min(5, starRating))}{'☆'.repeat(5 - Math.min(5, starRating))}</span>
              <span>STAR PLAYER</span>
            </p>
          </>
        ) : (
          <p className="text-xs md:text-sm text-slate-400 leading-snug">0 stars — play a matchday to get rated</p>
        )}
      </div>
      <nav className="flex-1 px-4 space-y-1 mt-4">
        <NavLink to="/dashboard" className={({ isActive }) => `${navClass} ${isActive ? navActive : ''}`} onClick={closeMenu}>
          <span className="material-symbols-outlined shrink-0">dashboard</span>
          <span className="text-sm">Dashboard</span>
        </NavLink>
        <NavLink to="/matchday" className={({ isActive }) => `${navClass} ${isActive ? navActive : ''}`} onClick={closeMenu}>
          <span className="material-symbols-outlined shrink-0">calendar_month</span>
          <span className="text-sm">Matchday</span>
        </NavLink>
        <NavLink to="/leaderboard" className={({ isActive }) => `${navClass} ${isActive ? navActive : ''}`} onClick={closeMenu}>
          <span className="material-symbols-outlined shrink-0">leaderboard</span>
          <span className="text-sm">Leaderboard</span>
        </NavLink>
        <NavLink to="/rules" className={({ isActive }) => `${navClass} ${isActive ? navActive : ''}`} onClick={closeMenu}>
          <span className="material-symbols-outlined shrink-0">rule</span>
          <span className="text-sm">Rules</span>
        </NavLink>
        <NavLink to="/profile" className={({ isActive }) => `${navClass} ${isActive ? navActive : ''}`} onClick={closeMenu}>
          <span className="material-symbols-outlined shrink-0">manage_accounts</span>
          <span className="text-sm">My Profile</span>
        </NavLink>
      </nav>
      <div className="p-4 border-t border-primary/10 space-y-2">
        <button
          type="button"
          onClick={() => setDark((d) => !d)}
          className="flex items-center gap-3 w-full px-3 py-2.5 rounded-lg text-slate-400 hover:bg-primary/5 hover:text-primary transition-colors touch-manipulation"
          aria-label="Toggle dark mode"
        >
          <span className="material-symbols-outlined text-xl">{dark ? 'light_mode' : 'dark_mode'}</span>
          <span className="text-sm">{dark ? 'Light mode' : 'Dark mode'}</span>
        </button>
        <div className="flex items-center gap-3 p-2 rounded-xl bg-primary/5">
          <Link to="/profile" onClick={closeMenu} className="flex items-center gap-3 flex-1 min-w-0 hover:opacity-80 transition-opacity" title="Edit profile">
            <JerseyAvatar shortName={player.baller_name || player.first_name} number={player.jersey_number} />
            <div className="min-w-0">
              <p className="text-sm font-bold truncate">{player.baller_name}</p>
              <p className="text-xs text-slate-500 truncate">{player.first_name} {player.surname}</p>
            </div>
          </Link>
          <button type="button" onClick={handleLogout} className="min-w-[44px] min-h-[44px] flex items-center justify-center text-slate-400 hover:text-primary touch-manipulation" aria-label="Log out">
            <span className="material-symbols-outlined text-xl">logout</span>
          </button>
        </div>
      </div>
    </>
  );

  return (
    <div className="bg-background-light dark:bg-background-dark text-slate-900 dark:text-slate-100 min-h-screen min-h-[100dvh] flex overflow-hidden font-display">
      {/* Mobile menu backdrop */}
      <div
        className="fixed inset-0 bg-black/50 z-40 md:hidden transition-opacity"
        style={{ opacity: menuOpen ? 1 : 0, pointerEvents: menuOpen ? 'auto' : 'none' }}
        onClick={closeMenu}
        aria-hidden="true"
      />
      {/* Sidebar: drawer on mobile (fixed = out of flow), inline on desktop */}
      <aside
        className={`
          flex flex-col h-screen z-50
          fixed inset-y-0 left-0 w-64 transform transition-transform duration-200 ease-out
          md:relative md:translate-x-0 md:shrink-0 md:w-64
          border-r border-primary/10 bg-background-light dark:bg-background-dark
          ${menuOpen ? 'translate-x-0' : '-translate-x-full'}
        `}
      >
        <SidebarContent />
      </aside>
      {/* Main: mobile header + content (full width on mobile; flex-1 when sidebar visible on desktop) */}
      <div className="w-full md:min-w-0 flex-1 flex flex-col">
        <header className="md:hidden shrink-0 h-14 px-4 flex items-center gap-3 border-b border-primary/10 bg-background-light dark:bg-background-dark sticky top-0 z-30 safe-area-inset-top">
          <button
            type="button"
            onClick={() => setMenuOpen(true)}
            className="min-w-[44px] min-h-[44px] flex items-center justify-center text-slate-400 hover:text-primary touch-manipulation rounded-lg -ml-2"
            aria-label="Open menu"
          >
            <span className="material-symbols-outlined text-2xl">menu</span>
          </button>
          <span className="font-bold text-lg truncate">Eko Football</span>
        </header>
        <main className="flex-1 overflow-y-auto overflow-x-hidden custom-scrollbar flex flex-col min-w-0">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
