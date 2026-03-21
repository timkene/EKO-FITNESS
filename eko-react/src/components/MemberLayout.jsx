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

  // Base nav item — always-dark sidebar
  const navClass = "flex items-center gap-3 px-3 py-3 min-h-[44px] rounded-xl text-white hover:bg-white/5 hover:text-primary transition-all touch-manipulation";
  const navActive = "bg-primary/15 text-primary font-semibold border border-primary/20 shadow-sm shadow-primary/10";

  const SidebarContent = () => (
    <>
      {/* Logo */}
      <div className="p-5 flex items-center gap-3 border-b border-[#1e2433]">
        <div className="bg-primary size-10 rounded-xl flex items-center justify-center text-black shrink-0 shadow-lg shadow-primary/30">
          <span className="material-symbols-outlined font-bold text-2xl">sports_soccer</span>
        </div>
        <div className="min-w-0">
          <h1 className="text-base font-black leading-tight text-white tracking-tight">Eko Football</h1>
          <p className="text-[10px] text-primary font-bold uppercase tracking-widest">Player Portal</p>
        </div>
      </div>

      {/* Star rating badge */}
      <div className="px-4 py-3 mx-3 mt-3 rounded-xl bg-amber-500/8 border border-amber-500/15 flex flex-col justify-center min-h-[52px]">
        {typeof starRating === 'number' ? (
          <>
            <p className="text-[9px] font-black text-amber-500 uppercase tracking-widest mb-0.5">
              {starRating >= 1 ? 'You are a' : 'Player rating'}
            </p>
            <p className="text-sm font-black text-amber-400 flex items-center gap-1 leading-tight">
              <span className="inline-flex text-xs" aria-hidden>
                {'★'.repeat(Math.min(5, starRating))}{'☆'.repeat(5 - Math.min(5, starRating))}
              </span>
              <span>{starRating >= 1 ? 'STAR PLAYER' : 'Unranked'}</span>
            </p>
          </>
        ) : (
          <p className="text-xs text-white/50 leading-snug">Play a matchday to get rated</p>
        )}
      </div>

      {/* Navigation */}
      <nav className="flex-1 px-3 space-y-0.5 mt-4">
        {[
          { to: '/dashboard', icon: 'dashboard',       label: 'Dashboard' },
          { to: '/matchday',  icon: 'calendar_month',  label: 'Matchday' },
          { to: '/leaderboard',icon: 'leaderboard',    label: 'Leaderboard' },
          { to: '/rules',     icon: 'rule',             label: 'Rules' },
          { to: '/jersey',    icon: 'checkroom',        label: 'My Jersey' },
          { to: '/avatar',    icon: 'face',             label: 'My Avatar' },
          { to: '/profile',   icon: 'manage_accounts',  label: 'My Profile' },
        ].map(({ to, icon, label }) => (
          <NavLink
            key={to}
            to={to}
            className={({ isActive }) => `${navClass} ${isActive ? navActive : ''}`}
            onClick={closeMenu}
          >
            <span className="material-symbols-outlined shrink-0 text-[20px]">{icon}</span>
            <span className="text-sm">{label}</span>
          </NavLink>
        ))}
      </nav>

      {/* Bottom: dark mode + user */}
      <div className="p-3 border-t border-[#1e2433] space-y-2">
        <button
          type="button"
          onClick={() => setDark((d) => !d)}
          className="flex items-center gap-3 w-full px-3 py-2.5 rounded-xl text-white hover:bg-white/5 hover:text-primary transition-colors touch-manipulation"
          aria-label="Toggle dark mode"
        >
          <span className="material-symbols-outlined text-xl">{dark ? 'light_mode' : 'dark_mode'}</span>
          <span className="text-xs font-medium">{dark ? 'Light mode' : 'Dark mode'}</span>
        </button>
        <div className="flex items-center gap-3 p-2.5 rounded-xl bg-white/3 border border-[#1e2433]">
          <Link to="/profile" onClick={closeMenu} className="flex items-center gap-3 flex-1 min-w-0 hover:opacity-80 transition-opacity" title="Edit profile">
            <JerseyAvatar shortName={player.baller_name || player.first_name} number={player.jersey_number} />
            <div className="min-w-0">
              <p className="text-sm font-bold truncate text-white">{player.baller_name}</p>
              <p className="text-[10px] text-white/50 uppercase truncate">Pro Member</p>
            </div>
          </Link>
          <button type="button" onClick={handleLogout} className="min-w-[40px] min-h-[40px] flex items-center justify-center text-white/50 hover:text-primary touch-manipulation rounded-lg hover:bg-white/5 transition-colors" aria-label="Log out">
            <span className="material-symbols-outlined text-xl">logout</span>
          </button>
        </div>
      </div>
    </>
  );

  return (
    <div className="bg-background-light dark:bg-background-dark text-slate-900 dark:text-slate-100 min-h-screen min-h-[100dvh] flex overflow-hidden font-display">
      {/* Mobile backdrop */}
      <div
        className="fixed inset-0 bg-black/60 z-40 md:hidden transition-opacity"
        style={{ opacity: menuOpen ? 1 : 0, pointerEvents: menuOpen ? 'auto' : 'none' }}
        onClick={closeMenu}
        aria-hidden="true"
      />
      {/* Sidebar — always dark regardless of page theme */}
      <aside
        className={`
          flex flex-col h-screen z-50 text-white
          fixed inset-y-0 left-0 w-64 transform transition-transform duration-200 ease-out
          md:relative md:translate-x-0 md:shrink-0 md:w-64
          border-r border-[#1e2433] bg-[#0d1117]
          ${menuOpen ? 'translate-x-0' : '-translate-x-full'}
        `}
      >
        <SidebarContent />
      </aside>

      {/* Main content */}
      <div className="w-full md:min-w-0 flex-1 flex flex-col">
        {/* Mobile header */}
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
