import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';

// ── Helpers ────────────────────────────────────────────────────────────────────
function isLight(hex) {
  const c = hex.replace('#', '');
  const r = parseInt(c.slice(0,2),16), g = parseInt(c.slice(2,4),16), b = parseInt(c.slice(4,6),16);
  return (r*299 + g*587 + b*114) / 1000 > 160;
}

// ── Team catalogue ─────────────────────────────────────────────────────────────
const LEAGUES = [
  { id: 'epl',        label: 'EPL',        emoji: '🏴󠁧󠁢󠁥󠁮󠁧󠁿' },
  { id: 'laliga',     label: 'La Liga',    emoji: '🇪🇸' },
  { id: 'ucl',        label: 'UCL',        emoji: '⭐' },
  { id: 'bundesliga', label: 'Bundesliga', emoji: '🇩🇪' },
  { id: 'seriea',     label: 'Serie A',    emoji: '🇮🇹' },
];

// sportsDbId: TheSportsDB numeric team ID — used to fetch kit images directly
// pattern: 'stripes_v' (vertical), 'stripes_h' (hoops), 'sash' (diagonal), null (plain)
// brand: 'adidas'|'nike'|'puma'
const TEAMS = {
  epl: [
    { id:'arsenal',   name:'Arsenal',    sportsDbId:'133604', kits:{ home:{main:'#EF0107',sleeve:'#FFFFFF',collar:'#FFFFFF',brand:'adidas',collarStyle:'crew',pattern:null},         away:{main:'#FFFFFF',sleeve:'#FFFFFF',collar:'#EF0107',brand:'adidas',collarStyle:'crew',pattern:null},      third:{main:'#063672',sleeve:'#063672',collar:'#EF0107',brand:'adidas',collarStyle:'v',pattern:null} }},
    { id:'mancity',   name:'Man City',   sportsDbId:'133615', kits:{ home:{main:'#6CABDD',sleeve:'#6CABDD',collar:'#FFFFFF',brand:'puma',collarStyle:'v',pattern:null},               away:{main:'#FFFFFF',sleeve:'#FFFFFF',collar:'#6CABDD',brand:'puma',collarStyle:'crew',pattern:null},       third:{main:'#1C2C5B',sleeve:'#1C2C5B',collar:'#6CABDD',brand:'puma',collarStyle:'crew',pattern:null} }},
    { id:'liverpool', name:'Liverpool',  sportsDbId:'133602', kits:{ home:{main:'#C8102E',sleeve:'#C8102E',collar:'#00B2A9',brand:'nike',collarStyle:'crew',pattern:null},             away:{main:'#F6EB61',sleeve:'#F6EB61',collar:'#C8102E',brand:'nike',collarStyle:'v',pattern:null},          third:{main:'#00B2A9',sleeve:'#00B2A9',collar:'#C8102E',brand:'nike',collarStyle:'crew',pattern:null} }},
    { id:'chelsea',   name:'Chelsea',    sportsDbId:'133610', kits:{ home:{main:'#034694',sleeve:'#034694',collar:'#FFFFFF',brand:'nike',collarStyle:'crew',pattern:null},             away:{main:'#F5F5F5',sleeve:'#F5F5F5',collar:'#034694',brand:'nike',collarStyle:'crew',pattern:null},        third:{main:'#1A1A2E',sleeve:'#1A1A2E',collar:'#034694',brand:'nike',collarStyle:'crew',pattern:null} }},
    { id:'united',    name:'Man United', sportsDbId:'133616', kits:{ home:{main:'#DA291C',sleeve:'#000000',collar:'#FFE600',brand:'adidas',collarStyle:'crew',pattern:null},           away:{main:'#FFFFFF',sleeve:'#FFFFFF',collar:'#DA291C',brand:'adidas',collarStyle:'v',pattern:null},         third:{main:'#00ABE8',sleeve:'#00ABE8',collar:'#DA291C',brand:'adidas',collarStyle:'crew',pattern:null} }},
    { id:'spurs',     name:'Tottenham',  sportsDbId:'133612', kits:{ home:{main:'#FFFFFF',sleeve:'#001C58',collar:'#001C58',brand:'nike',collarStyle:'crew',pattern:null},             away:{main:'#001C58',sleeve:'#001C58',collar:'#FFFFFF',brand:'nike',collarStyle:'crew',pattern:null},         third:{main:'#E11E20',sleeve:'#E11E20',collar:'#FFFFFF',brand:'nike',collarStyle:'crew',pattern:null} }},
  ],
  laliga: [
    { id:'realmadrid',name:'Real Madrid', sportsDbId:'133738', kits:{ home:{main:'#FFFFFF',sleeve:'#FFFFFF',collar:'#F0BC42',brand:'adidas',collarStyle:'crew',pattern:null},          away:{main:'#7B3F8C',sleeve:'#7B3F8C',collar:'#F0BC42',brand:'adidas',collarStyle:'v',pattern:null},          third:{main:'#1E1E1E',sleeve:'#1E1E1E',collar:'#FFFFFF',brand:'adidas',collarStyle:'crew',pattern:null} }},
    { id:'barcelona', name:'Barcelona',   sportsDbId:'133739', kits:{ home:{main:'#A50044',sleeve:'#004D98',collar:'#A50044',brand:'nike',collarStyle:'crew',pattern:'stripes_v',stripe:'#004D98'},  away:{main:'#EDBB00',sleeve:'#EDBB00',collar:'#004D98',brand:'nike',collarStyle:'v',pattern:null},       third:{main:'#101921',sleeve:'#004D98',collar:'#004D98',brand:'nike',collarStyle:'crew',pattern:null} }},
    { id:'atletico',  name:'Atlético',    sportsDbId:'133736', kits:{ home:{main:'#CB3524',sleeve:'#FFFFFF',collar:'#FFFFFF',brand:'nike',collarStyle:'crew',pattern:'stripes_v',stripe:'#FFFFFF'},  away:{main:'#FFFFFF',sleeve:'#FFFFFF',collar:'#CB3524',brand:'nike',collarStyle:'crew',pattern:null},     third:{main:'#1C1C1C',sleeve:'#CB3524',collar:'#CB3524',brand:'nike',collarStyle:'crew',pattern:null} }},
  ],
  ucl: [
    { id:'psg',       name:'PSG',         sportsDbId:'133714', kits:{ home:{main:'#004170',sleeve:'#004170',collar:'#ED1C24',brand:'nike',collarStyle:'crew',pattern:null},             away:{main:'#FFFFFF',sleeve:'#FFFFFF',collar:'#004170',brand:'nike',collarStyle:'v',pattern:null},             third:{main:'#000000',sleeve:'#000000',collar:'#ED1C24',brand:'nike',collarStyle:'crew',pattern:null} }},
    { id:'inter',     name:'Inter Milan', sportsDbId:'133667', kits:{ home:{main:'#010E80',sleeve:'#010E80',collar:'#FFFFFF',brand:'nike',collarStyle:'crew',pattern:'stripes_v',stripe:'#000000'}, away:{main:'#FFFFFF',sleeve:'#FFFFFF',collar:'#010E80',brand:'nike',collarStyle:'crew',pattern:null},    third:{main:'#2B5C34',sleeve:'#2B5C34',collar:'#FFFFFF',brand:'nike',collarStyle:'crew',pattern:null} }},
    { id:'ajax',      name:'Ajax',        sportsDbId:'133707', kits:{ home:{main:'#FFFFFF',sleeve:'#D2122E',collar:'#FFFFFF',brand:'adidas',collarStyle:'crew',pattern:null},           away:{main:'#D2122E',sleeve:'#FFFFFF',collar:'#D2122E',brand:'adidas',collarStyle:'crew',pattern:null},           third:{main:'#000000',sleeve:'#000000',collar:'#D2122E',brand:'adidas',collarStyle:'crew',pattern:null} }},
  ],
  bundesliga: [
    { id:'bayern',    name:'Bayern',      sportsDbId:'133664', kits:{ home:{main:'#DC052D',sleeve:'#DC052D',collar:'#FFFFFF',brand:'adidas',collarStyle:'crew',pattern:null},           away:{main:'#FFFFFF',sleeve:'#FFFFFF',collar:'#DC052D',brand:'adidas',collarStyle:'crew',pattern:null},           third:{main:'#0066B2',sleeve:'#0066B2',collar:'#DC052D',brand:'adidas',collarStyle:'crew',pattern:null} }},
    { id:'dortmund',  name:'Dortmund',    sportsDbId:'133650', kits:{ home:{main:'#FDE100',sleeve:'#FDE100',collar:'#000000',brand:'puma',collarStyle:'crew',pattern:null},             away:{main:'#000000',sleeve:'#000000',collar:'#FDE100',brand:'puma',collarStyle:'v',pattern:null},               third:{main:'#5A2D82',sleeve:'#5A2D82',collar:'#FDE100',brand:'puma',collarStyle:'crew',pattern:null} }},
    { id:'leverkusen',name:'Leverkusen',  sportsDbId:'133663', kits:{ home:{main:'#E32221',sleeve:'#000000',collar:'#FFFFFF',brand:'adidas',collarStyle:'crew',pattern:null},           away:{main:'#FFFFFF',sleeve:'#FFFFFF',collar:'#E32221',brand:'adidas',collarStyle:'crew',pattern:null},           third:{main:'#1C1C1C',sleeve:'#E32221',collar:'#E32221',brand:'adidas',collarStyle:'crew',pattern:null} }},
  ],
  seriea: [
    { id:'juventus',  name:'Juventus',    sportsDbId:'133676', kits:{ home:{main:'#FFFFFF',sleeve:'#000000',collar:'#000000',brand:'adidas',collarStyle:'crew',pattern:'stripes_v',stripe:'#000000'}, away:{main:'#000000',sleeve:'#000000',collar:'#FFFFFF',brand:'adidas',collarStyle:'v',pattern:null}, third:{main:'#F7A8C9',sleeve:'#F7A8C9',collar:'#000000',brand:'adidas',collarStyle:'crew',pattern:null} }},
    { id:'acmilan',   name:'AC Milan',    sportsDbId:'133670', kits:{ home:{main:'#FB090B',sleeve:'#000000',collar:'#000000',brand:'puma',collarStyle:'v',pattern:'stripes_v',stripe:'#000000'},       away:{main:'#FFFFFF',sleeve:'#000000',collar:'#FB090B',brand:'puma',collarStyle:'crew',pattern:null},     third:{main:'#1C1C1C',sleeve:'#FB090B',collar:'#FB090B',brand:'puma',collarStyle:'crew',pattern:null} }},
    { id:'intermilan',name:'Inter Milan', sportsDbId:'133667', kits:{ home:{main:'#010E80',sleeve:'#010E80',collar:'#FFFFFF',brand:'nike',collarStyle:'crew',pattern:'stripes_v',stripe:'#000000'},     away:{main:'#FFFFFF',sleeve:'#FFFFFF',collar:'#010E80',brand:'nike',collarStyle:'crew',pattern:null},     third:{main:'#2B5C34',sleeve:'#2B5C34',collar:'#FFFFFF',brand:'nike',collarStyle:'crew',pattern:null} }},
    { id:'napoli',    name:'Napoli',      sportsDbId:'133669', kits:{ home:{main:'#0066B3',sleeve:'#0066B3',collar:'#FFFFFF',brand:'emporio',collarStyle:'crew',pattern:null},           away:{main:'#FFFFFF',sleeve:'#FFFFFF',collar:'#0066B3',brand:'emporio',collarStyle:'crew',pattern:null},           third:{main:'#000000',sleeve:'#000000',collar:'#0066B3',brand:'emporio',collarStyle:'crew',pattern:null} }},
  ],
};

const KIT_LABELS = { home: 'Home', away: 'Away', third: 'Third' };
const STORAGE_KEY = 'eko_jersey';

// ── High-quality SVG jersey ────────────────────────────────────────────────────
export function JerseySVG({ kit, playerName, playerNumber, size = 260 }) {
  if (!kit) return null;
  const { main, sleeve, collar, pattern, stripe, brand, collarStyle = 'crew' } = kit;
  const light = isLight(main);
  const textFill = light ? 'rgba(0,0,0,0.80)' : 'rgba(255,255,255,0.92)';
  const sid = `j${Math.random().toString(36).slice(2,8)}`;

  // Nike swoosh path (right chest area)
  const nikePath = `M128,54 C135,50 145,48 148,52 C150,55 146,58 128,54 Z`;
  // Adidas 3-stripe mark (shoulder)
  const adiStripes = [
    [130,46,144,46],[132,50,146,50],[134,54,148,54]
  ];

  return (
    <svg
      width={size}
      height={size * 1.15}
      viewBox="0 0 200 230"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      style={{ filter:'drop-shadow(0 16px 32px rgba(0,0,0,0.45)) drop-shadow(0 4px 8px rgba(0,0,0,0.3))' }}
    >
      <defs>
        {/* ── Vertical stripe pattern ── */}
        {pattern === 'stripes_v' && stripe && (
          <pattern id={`sv${sid}`} x="0" y="0" width="20" height="1" patternUnits="userSpaceOnUse">
            <rect width="10" height="1" fill={main}/>
            <rect x="10" width="10" height="1" fill={stripe}/>
          </pattern>
        )}
        {/* ── 3-D fabric gradient (radial from upper-left) ── */}
        <radialGradient id={`rg${sid}`} cx="30%" cy="25%" r="75%">
          <stop offset="0%" stopColor="rgba(255,255,255,0.25)"/>
          <stop offset="60%" stopColor="rgba(255,255,255,0)"/>
          <stop offset="100%" stopColor="rgba(0,0,0,0.18)"/>
        </radialGradient>
        {/* ── Side-edge shadow ── */}
        <linearGradient id={`ss${sid}`} x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%"   stopColor="rgba(0,0,0,0.28)"/>
          <stop offset="14%"  stopColor="rgba(0,0,0,0)"/>
          <stop offset="86%"  stopColor="rgba(0,0,0,0)"/>
          <stop offset="100%" stopColor="rgba(0,0,0,0.22)"/>
        </linearGradient>
        {/* ── Bottom fade ── */}
        <linearGradient id={`bf${sid}`} x1="0%" y1="0%" x2="0%" y2="100%">
          <stop offset="60%" stopColor="rgba(0,0,0,0)"/>
          <stop offset="100%" stopColor="rgba(0,0,0,0.12)"/>
        </linearGradient>
        {/* ── Chest sheen ── */}
        <linearGradient id={`cs${sid}`} x1="25%" y1="0%" x2="55%" y2="90%">
          <stop offset="0%" stopColor="rgba(255,255,255,0.20)"/>
          <stop offset="100%" stopColor="rgba(255,255,255,0)"/>
        </linearGradient>
      </defs>

      {/* ═══ LEFT SLEEVE ═══ */}
      <path
        d="M7 46 L0 82 Q0 88 6 88 L51 76 L47 42 Z"
        fill={pattern === 'stripes_v' ? `url(#sv${sid})` : sleeve}
      />
      {/* ═══ RIGHT SLEEVE ═══ */}
      <path
        d="M193 46 L200 82 Q200 88 194 88 L149 76 L153 42 Z"
        fill={pattern === 'stripes_v' ? `url(#sv${sid})` : sleeve}
      />
      {/* ═══ BODY ═══ */}
      <path
        d="M47 42 L51 76 L37 218 Q37 226 47 226 L153 226 Q163 226 163 218 L149 76 L153 42 Q132 30 100 28 Q68 30 47 42 Z"
        fill={pattern === 'stripes_v' ? `url(#sv${sid})` : main}
      />

      {/* ═══ SHADING LAYERS ═══ */}
      {/* Fabric 3D */}
      <path d="M47 42 L51 76 L37 218 Q37 226 47 226 L153 226 Q163 226 163 218 L149 76 L153 42 Q132 30 100 28 Q68 30 47 42 Z" fill={`url(#rg${sid})`} opacity="0.65"/>
      {/* Side shadows */}
      <path d="M47 42 L51 76 L37 218 Q37 226 47 226 L153 226 Q163 226 163 218 L149 76 L153 42 Q132 30 100 28 Q68 30 47 42 Z" fill={`url(#ss${sid})`} opacity="0.55"/>
      {/* Bottom fade */}
      <path d="M47 42 L51 76 L37 218 Q37 226 47 226 L153 226 Q163 226 163 218 L149 76 L153 42 Q132 30 100 28 Q68 30 47 42 Z" fill={`url(#bf${sid})`}/>
      {/* Chest sheen */}
      <path d="M47 42 L51 76 L37 218 Q37 226 47 226 L153 226 Q163 226 163 218 L149 76 L153 42 Q132 30 100 28 Q68 30 47 42 Z" fill={`url(#cs${sid})`}/>
      {/* Sleeve shading */}
      <path d="M7 46 L0 82 Q0 88 6 88 L51 76 L47 42 Z" fill={`url(#ss${sid})`} opacity="0.4"/>
      <path d="M193 46 L200 82 Q200 88 194 88 L149 76 L153 42 Z" fill={`url(#ss${sid})`} opacity="0.4"/>

      {/* ═══ COLLAR ═══ */}
      {collarStyle === 'v' ? (
        <path
          d="M82 28 Q91 68 100 74 Q109 68 118 28 Q113 20 100 20 Q87 20 82 28 Z"
          fill={collar}
          stroke={main === '#FFFFFF' ? 'rgba(0,0,0,0.12)' : 'none'}
          strokeWidth="0.5"
        />
      ) : (
        /* Crew/round collar */
        <ellipse
          cx="100" cy="28" rx="20" ry="11"
          fill={collar}
          stroke={main === '#FFFFFF' ? 'rgba(0,0,0,0.12)' : 'none'}
          strokeWidth="0.5"
        />
      )}

      {/* ═══ BRAND LOGO ═══ */}
      {brand === 'adidas' && adiStripes.map(([x1,y1,x2,y2],i) => (
        <line key={i} x1={x1} y1={y1} x2={x2} y2={y2} stroke="rgba(255,255,255,0.70)" strokeWidth="2.2" strokeLinecap="round"/>
      ))}
      {brand === 'nike' && (
        <path d={nikePath} fill="rgba(255,255,255,0.65)"/>
      )}
      {brand === 'puma' && (
        <path d="M130 50 L142 46 L145 52 L133 56 Z" fill="rgba(255,255,255,0.60)"/>
      )}

      {/* ═══ CREST CIRCLE ═══ */}
      <circle cx="70" cy="92" r="13" fill="rgba(255,255,255,0.18)" stroke="rgba(255,255,255,0.35)" strokeWidth="1.5"/>
      <text x="70" y="97" textAnchor="middle" fontSize="9" fill="rgba(255,255,255,0.55)" fontFamily="Lexend,sans-serif">FC</text>

      {/* ═══ PLAYER NAME ═══ */}
      {playerName && (
        <text
          x="100" y="162"
          textAnchor="middle"
          fontSize="13" fontWeight="900"
          fontFamily="Lexend,Arial Narrow,sans-serif"
          letterSpacing="3"
          fill={textFill}
        >
          {playerName.toUpperCase().slice(0,11)}
        </text>
      )}

      {/* ═══ PLAYER NUMBER ═══ */}
      {playerNumber && (
        <text
          x="100" y="213"
          textAnchor="middle"
          fontSize="52" fontWeight="900"
          fontFamily="Lexend,sans-serif"
          fill={textFill}
        >
          {playerNumber}
        </text>
      )}
    </svg>
  );
}

// ── Persistence helpers ────────────────────────────────────────────────────────
export function getSavedJersey() {
  try { return JSON.parse(localStorage.getItem(STORAGE_KEY)); } catch { return null; }
}

// ── TheSportsDB helpers ────────────────────────────────────────────────────────
// Patreon key takes priority; "123" is the correct free API key
const API_KEY = import.meta.env.VITE_THESPORTSDB_KEY || '123';
const BASE    = `https://www.thesportsdb.com/api/v1/json/${API_KEY}`;

// Full club names for display in hero
export const FULL_TEAM_NAMES = {
  arsenal:   'Arsenal F.C.',
  mancity:   'Manchester City F.C.',
  liverpool: 'Liverpool F.C.',
  chelsea:   'Chelsea F.C.',
  united:    'Manchester United F.C.',
  spurs:     'Tottenham Hotspur F.C.',
  realmadrid:'Real Madrid C.F.',
  barcelona: 'FC Barcelona',
  atletico:  'Atlético de Madrid',
  psg:       'Paris Saint-Germain F.C.',
  inter:     'FC Internazionale',
  ajax:      'AFC Ajax',
  bayern:    'FC Bayern München',
  dortmund:  'Borussia Dortmund',
  leverkusen:'Bayer 04 Leverkusen',
  juventus:  'Juventus F.C.',
  acmilan:   'AC Milan',
  intermilan:'FC Internazionale',
  napoli:    'SSC Napoli',
};

// In-memory cache — keyed by `${sportsDbId}|${kitLabel}`
const _kitCache = {};
// In-memory cache for team fanart
const _fanartCache = {};

// API uses strType: "1st"|"2nd"|"3rd" and strEquipment for the image URL
const KIT_TYPE_MAP = { Home: '1st', Away: '2nd', Third: '3rd' };

export async function fetchTeamFanart(sportsDbId) {
  if (!sportsDbId) return null;
  if (_fanartCache[sportsDbId] !== undefined) return _fanartCache[sportsDbId];
  try {
    const r = await fetch(`${BASE}/lookupteam.php?id=${sportsDbId}`);
    const d = await r.json();
    const t = d?.teams?.[0];
    const url = t?.strTeamFanart1 || t?.strTeamFanart2 || t?.strTeamBanner || t?.strStadiumThumb || null;
    _fanartCache[sportsDbId] = url;
    return url;
  } catch {
    _fanartCache[sportsDbId] = null;
    return null;
  }
}

export async function fetchKitImage(sportsDbId, kitTypeLabel) {
  if (!sportsDbId) return null;
  const cacheKey = `${sportsDbId}|${kitTypeLabel}`;
  if (_kitCache[cacheKey] !== undefined) return _kitCache[cacheKey];

  try {
    const er = await fetch(`${BASE}/lookupequipment.php?id=${sportsDbId}`);
    const ed = await er.json();
    const equipment = ed.equipment;
    if (!equipment?.length) { _kitCache[cacheKey] = null; return null; }

    // Sort by most-recent season, then find matching kit type
    const strType = KIT_TYPE_MAP[kitTypeLabel] || '1st';
    const sorted = [...equipment].sort((a, b) =>
      (b.strSeason || '').localeCompare(a.strSeason || '')
    );
    const match = sorted.find(e => e.strType === strType) || sorted[0];
    const url = match?.strEquipment || null;
    _kitCache[cacheKey] = url;
    return url;
  } catch {
    _kitCache[cacheKey] = null;
    return null;
  }
}

// ── Main page ──────────────────────────────────────────────────────────────────
export default function JerseyDesigner() {
  const navigate = useNavigate();
  const saved = getSavedJersey();

  const [selectedLeague, setSelectedLeague] = useState(saved?.leagueId || 'epl');
  const [selectedTeamId, setSelectedTeamId] = useState(saved?.teamId || 'arsenal');
  const [kitType, setKitType]     = useState(saved?.kitType || 'home');
  const [playerName, setPlayerName]   = useState(saved?.playerName || '');
  const [playerNumber, setPlayerNumber] = useState(saved?.playerNumber || '');
  const [savedMsg, setSavedMsg]   = useState(false);
  const [kitImageUrl, setKitImageUrl] = useState(null);
  const [kitImageLoading, setKitImageLoading] = useState(false);

  const teams = TEAMS[selectedLeague] || [];
  const team  = teams.find(t => t.id === selectedTeamId) || teams[0];
  const kit   = team?.kits?.[kitType] || team?.kits?.home;

  // If switching league and current team not present, reset to first
  useEffect(() => {
    const inLeague = (TEAMS[selectedLeague] || []).find(t => t.id === selectedTeamId);
    if (!inLeague) setSelectedTeamId((TEAMS[selectedLeague] || [])[0]?.id);
  }, [selectedLeague]);

  // Fetch real kit image when team or kit type changes
  useEffect(() => {
    if (!team?.sportsDbId) return;
    const kitLabel = { home: 'Home', away: 'Away', third: 'Third' }[kitType] || 'Home';
    setKitImageLoading(true);
    setKitImageUrl(null);
    fetchKitImage(team.sportsDbId, kitLabel)
      .then(url => { setKitImageUrl(url); setKitImageLoading(false); })
      .catch(() => setKitImageLoading(false));
  }, [team?.sportsDbId, kitType]);

  const handleSave = () => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({
      leagueId: selectedLeague,
      teamId: team?.id,
      teamName: team?.name,
      teamColor: kit?.main,
      sportsDbId: team?.sportsDbId,
      kitType,
      playerName,
      playerNumber,
      kit,
    }));
    setSavedMsg(true);
    setTimeout(() => setSavedMsg(false), 2200);
  };

  return (
    <div className="bg-background-light dark:bg-background-dark text-slate-900 dark:text-slate-100 min-h-screen flex flex-col font-display">
      {/* Header */}
      <header className="flex items-center justify-between px-4 py-4 border-b border-slate-200 dark:border-slate-800 sticky top-0 z-10 bg-background-light dark:bg-background-dark">
        <button onClick={() => navigate('/dashboard')} className="p-2 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-full">
          <span className="material-symbols-outlined">arrow_back</span>
        </button>
        <h1 className="text-lg font-bold">Jersey Designer</h1>
        <button
          onClick={handleSave}
          className={`px-3 py-1.5 rounded-lg text-sm font-bold transition-all ${savedMsg ? 'bg-primary/20 text-primary' : 'bg-primary text-white'}`}
        >
          {savedMsg ? '✓ Saved' : 'Save'}
        </button>
      </header>

      <main className="flex-1 overflow-y-auto pb-24">
        {/* ── Jersey Preview ── */}
        <section className="p-4">
          <div
            className="relative w-full rounded-xl overflow-hidden flex flex-col items-center justify-center py-6"
            style={{
              minHeight: 320,
              background: `radial-gradient(circle at 40% 35%, ${kit?.main || '#0ac247'}28 0%, transparent 65%), #0a0e0a`,
            }}
          >
            {/* Ambient glow behind jersey */}
            <div
              className="absolute size-52 rounded-full blur-3xl opacity-20 pointer-events-none"
              style={{ backgroundColor: kit?.main || '#0ac247' }}
            />
            <div className="relative z-10 flex flex-col items-center gap-1">
              {/* Name + number always shown above the jersey */}
              {(playerName || playerNumber) && (
                <div className="flex flex-col items-center leading-none mb-1">
                  {playerName && (
                    <span className="font-black tracking-[0.22em] text-white uppercase" style={{ fontSize: 18, textShadow: '0 2px 8px rgba(0,0,0,0.7)' }}>
                      {playerName.toUpperCase().slice(0, 11)}
                    </span>
                  )}
                  {playerNumber && (
                    <span className="font-black text-white leading-none" style={{ fontSize: 52, lineHeight: 1, textShadow: '0 2px 12px rgba(0,0,0,0.7)' }}>
                      {playerNumber}
                    </span>
                  )}
                </div>
              )}

              {kitImageLoading ? (
                <div className="flex flex-col items-center justify-center" style={{ width: 240, height: 220 }}>
                  <div className="size-12 rounded-full border-4 border-slate-700 border-t-primary animate-spin mb-3" />
                  <p className="text-xs text-slate-500">Fetching kit…</p>
                </div>
              ) : kitImageUrl ? (
                <img
                  src={kitImageUrl}
                  alt={`${team?.name} ${kitType} kit`}
                  className="object-contain drop-shadow-2xl"
                  style={{ width: 240, height: 220 }}
                  onError={() => setKitImageUrl(null)}
                />
              ) : (
                <JerseySVG kit={kit} playerName={playerName} playerNumber={playerNumber} size={220} />
              )}
            </div>
          </div>

          {/* Kit type toggle */}
          <div className="flex gap-1.5 bg-slate-900/70 backdrop-blur-md p-1.5 rounded-full border border-slate-700 w-fit mx-auto mt-3">
            {Object.keys(KIT_LABELS).map(k => (
              <button
                key={k}
                onClick={() => setKitType(k)}
                className={`px-5 py-1.5 rounded-full text-xs font-semibold transition-all ${kitType === k ? 'bg-primary text-white' : 'text-slate-400 hover:text-white'}`}
              >
                {KIT_LABELS[k]}
              </button>
            ))}
          </div>
        </section>

        {/* ── League selector ── */}
        <section className="mt-2">
          <p className="px-4 text-xs font-bold uppercase tracking-widest text-slate-500 mb-3">Select League</p>
          <div className="flex overflow-x-auto gap-4 px-4 pb-2" style={{ scrollbarWidth:'none' }}>
            {LEAGUES.map(lg => (
              <button
                key={lg.id}
                onClick={() => setSelectedLeague(lg.id)}
                className={`flex flex-col items-center gap-1.5 shrink-0 transition-all ${selectedLeague === lg.id ? 'opacity-100' : 'opacity-50'}`}
              >
                <div className={`size-14 rounded-full flex items-center justify-center text-2xl border-2 transition-all ${selectedLeague === lg.id ? 'border-primary bg-primary/10' : 'border-transparent bg-slate-100 dark:bg-slate-800'}`}>
                  {lg.emoji}
                </div>
                <span className="text-xs font-medium">{lg.label}</span>
              </button>
            ))}
          </div>
        </section>

        {/* ── Club grid ── */}
        <section className="mt-6 px-4">
          <p className="text-xs font-bold uppercase tracking-widest text-slate-500 mb-4">Select Club</p>
          <div className="grid grid-cols-3 gap-3">
            {(TEAMS[selectedLeague] || []).map(t => {
              const previewKit = t.kits[kitType] || t.kits.home;
              const isSelected = selectedTeamId === t.id;
              return (
                <button
                  key={t.id}
                  onClick={() => setSelectedTeamId(t.id)}
                  className={`rounded-xl pt-3 pb-2 px-2 flex flex-col items-center gap-2 border-2 transition-all ${isSelected ? 'border-primary bg-primary/5' : 'border-transparent bg-slate-100 dark:bg-slate-800/60'}`}
                >
                  <JerseySVG kit={previewKit} size={64} />
                  <span className={`text-[11px] font-semibold text-center leading-tight ${isSelected ? 'text-primary' : 'text-slate-500'}`}>{t.name}</span>
                </button>
              );
            })}
          </div>
        </section>

        {/* ── Personalise ── */}
        <section className="mt-6 px-4">
          <p className="text-xs font-bold uppercase tracking-widest text-slate-500 mb-4">Personalise</p>
          <div className="flex gap-3">
            <div className="flex-1">
              <label className="text-[10px] text-slate-500 uppercase font-bold mb-1 block">Name on shirt</label>
              <input
                value={playerName}
                onChange={e => setPlayerName(e.target.value.slice(0,12).toUpperCase())}
                placeholder="YOUR NAME"
                maxLength={12}
                className="w-full rounded-lg bg-slate-100 dark:bg-slate-800 border border-slate-300 dark:border-slate-700 px-3 py-2.5 text-sm font-bold uppercase tracking-wider focus:border-primary focus:outline-none"
              />
            </div>
            <div className="w-24">
              <label className="text-[10px] text-slate-500 uppercase font-bold mb-1 block">Number</label>
              <input
                value={playerNumber}
                onChange={e => setPlayerNumber(e.target.value.replace(/\D/g,'').slice(0,2))}
                placeholder="7"
                maxLength={2}
                className="w-full rounded-lg bg-slate-100 dark:bg-slate-800 border border-slate-300 dark:border-slate-700 px-3 py-2.5 text-sm font-bold text-center focus:border-primary focus:outline-none"
              />
            </div>
          </div>
        </section>

        {/* ── Save CTA ── */}
        <div className="px-4 mt-6">
          <button
            onClick={handleSave}
            className={`w-full py-3.5 rounded-xl font-bold text-sm flex items-center justify-center gap-2 transition-all ${savedMsg ? 'bg-primary/20 text-primary' : 'bg-primary text-white'}`}
          >
            <span className="material-symbols-outlined text-base">checkroom</span>
            {savedMsg ? 'Jersey saved to your hub!' : 'Save to Player Hub'}
          </button>
        </div>
      </main>

    </div>
  );
}
