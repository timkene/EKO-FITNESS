import { useState, useId } from 'react';
import { useNavigate } from 'react-router-dom';
import { getSavedJersey } from './JerseyDesigner';

// ── Storage ────────────────────────────────────────────────────────────────────
const AVATAR_KEY = 'eko_avatar';
export function getSavedAvatar() {
  try { return JSON.parse(localStorage.getItem(AVATAR_KEY)); } catch { return null; }
}

// ── Options catalogue ──────────────────────────────────────────────────────────
export const SKIN_TONES = [
  { id: 'tone1', color: '#FDDBB4', label: 'Light' },
  { id: 'tone2', color: '#E8A87C', label: 'Medium Light' },
  { id: 'tone3', color: '#C68642', label: 'Medium' },
  { id: 'tone4', color: '#8D5524', label: 'Medium Dark' },
  { id: 'tone5', color: '#5C2D0E', label: 'Dark' },
  { id: 'tone6', color: '#2D1100', label: 'Very Dark' },
];
export const HAIR_STYLES = [
  { id: 'bald',   label: 'Bald' },
  { id: 'low',    label: 'Low Cut' },
  { id: 'crop',   label: 'Short Crop' },
  { id: 'afro',   label: 'Afro' },
  { id: 'mohawk', label: 'Mohawk' },
  { id: 'waves',  label: 'Waves' },
];
export const HAIR_COLORS = [
  { id: 'black',  color: '#1a1010' },
  { id: 'brown',  color: '#5C3317' },
  { id: 'blonde', color: '#C8A96E' },
  { id: 'gray',   color: '#888888' },
];
const EYE_STYLES = [
  { id: 'normal',  label: 'Normal' },
  { id: 'intense', label: 'Intense' },
  { id: 'relaxed', label: 'Relaxed' },
];
const BEARDS = [
  { id: 'none',    label: 'None' },
  { id: 'stubble', label: 'Stubble' },
  { id: 'goatee',  label: 'Goatee' },
  { id: 'full',    label: 'Full Beard' },
];

// ── Avatar SVG ─────────────────────────────────────────────────────────────────
export function AvatarSVG({ config = {}, kit = null, playerName = '', playerNumber = '', size = 240 }) {
  const sid = useId().replace(/:/g, '');
  const skin   = config.skinTone  || '#C68642';
  const hStyle = config.hairStyle || 'low';
  const hColor = config.hairColor || '#1a1010';
  const eStyle = config.eyeStyle  || 'normal';
  const beard  = config.beard     || 'none';

  const main    = kit?.main    || '#0ac247';
  const sleeve  = kit?.sleeve  || '#0ac247';
  const collar  = kit?.collar  || '#ffffff';
  const cStyle  = kit?.collarStyle || 'crew';

  const shortBg  = '#0d1420';
  const sockBg   = '#e8e8e8';
  const bootBg   = '#111111';

  return (
    <svg
      width={size} height={size * 1.25}
      viewBox="0 0 200 250" fill="none"
      xmlns="http://www.w3.org/2000/svg"
      style={{ filter: 'drop-shadow(0 12px 28px rgba(0,0,0,0.55)) drop-shadow(0 4px 8px rgba(0,0,0,0.3))' }}
    >
      <defs>
        <radialGradient id={`sg${sid}`} cx="38%" cy="30%" r="72%">
          <stop offset="0%" stopColor="rgba(255,255,255,0.18)"/>
          <stop offset="100%" stopColor="rgba(0,0,0,0.22)"/>
        </radialGradient>
        <radialGradient id={`jg${sid}`} cx="30%" cy="25%" r="75%">
          <stop offset="0%" stopColor="rgba(255,255,255,0.20)"/>
          <stop offset="100%" stopColor="rgba(0,0,0,0.28)"/>
        </radialGradient>
      </defs>

      {/* ── Boots ── */}
      <ellipse cx="76"  cy="242" rx="17" ry="7" fill={bootBg}/>
      <ellipse cx="124" cy="242" rx="17" ry="7" fill={bootBg}/>
      <ellipse cx="72"  cy="239" rx="6" ry="2.5" fill="rgba(255,255,255,0.13)" transform="rotate(-15 72 239)"/>
      <ellipse cx="120" cy="239" rx="6" ry="2.5" fill="rgba(255,255,255,0.13)" transform="rotate(-15 120 239)"/>

      {/* ── Socks ── */}
      <rect x="65"  y="200" width="22" height="42" rx="3" fill={sockBg}/>
      <rect x="113" y="200" width="22" height="42" rx="3" fill={sockBg}/>
      <rect x="65"  y="200" width="22" height="8"  rx="2" fill={main}/>
      <rect x="113" y="200" width="22" height="8"  rx="2" fill={main}/>

      {/* ── Shorts ── */}
      <path d="M 62 166 L 59 204 L 89 204 L 100 175 L 111 204 L 141 204 L 138 166 Z" fill={shortBg}/>
      <line x1="100" y1="175" x2="100" y2="204" stroke="rgba(255,255,255,0.08)" strokeWidth="1"/>
      <path d="M 62 166 L 59 174 L 89 174 L 100 166 Z" fill="rgba(255,255,255,0.05)"/>
      <path d="M 138 166 L 141 174 L 111 174 L 100 166 Z" fill="rgba(255,255,255,0.05)"/>

      {/* ── Jersey body ── */}
      <path d="M 57 91 L 49 166 L 151 166 L 143 91 Z" fill={main}/>
      <path d="M 57 91 L 49 166 L 151 166 L 143 91 Z" fill={`url(#jg${sid})`}/>

      {/* Jersey number on chest */}
      {playerNumber && (
        <text x="100" y="148" textAnchor="middle"
              fontFamily="'Arial Black',Arial,sans-serif"
              fontWeight="900" fontSize="20"
              fill="rgba(255,255,255,0.35)">
          {playerNumber}
        </text>
      )}

      {/* ── Upper arm stubs (behind folded arms) ── */}
      <path d="M 57 91 C 48 88 33 97 30 111 C 28 118 32 124 40 124 L 57 120 Z" fill={sleeve}/>
      <path d="M 143 91 C 152 88 167 97 170 111 C 172 118 168 124 160 124 L 143 120 Z" fill={sleeve}/>

      {/* ── Right forearm (lower layer, goes left) ── */}
      <path d="
        M 156 114
        C 158 110 155 106 151 107
        L 50 119
        C 46 120 43 123 44 127
        C 45 130 49 132 53 131
        L 153 121
        C 157 120 158 118 156 114 Z
      " fill={sleeve}/>
      {/* Right forearm shading */}
      <path d="M 151 107 L 53 131 C 49 132 45 130 44 127 C 43 124 44 121 47 120 L 151 108 Z"
            fill="rgba(0,0,0,0.1)"/>
      {/* Right hand */}
      <ellipse cx="48" cy="126" rx="10" ry="7.5" fill={skin}/>
      <ellipse cx="48" cy="126" rx="10" ry="7.5" fill={`url(#sg${sid})`}/>
      {/* Knuckle lines */}
      <line x1="43" y1="124" x2="43" y2="128" stroke="rgba(0,0,0,0.15)" strokeWidth="0.8"/>
      <line x1="46" y1="123" x2="46" y2="129" stroke="rgba(0,0,0,0.15)" strokeWidth="0.8"/>
      <line x1="49" y1="123" x2="49" y2="129" stroke="rgba(0,0,0,0.15)" strokeWidth="0.8"/>

      {/* ── Left forearm (upper layer, goes right) ── */}
      <path d="
        M 44 108
        C 42 104 45 100 49 101
        L 150 112
        C 154 113 157 116 156 120
        C 155 123 151 125 147 124
        L 47 115
        C 43 114 42 112 44 108 Z
      " fill={sleeve}/>
      {/* Left forearm highlight */}
      <path d="M 49 101 L 147 124 C 151 125 155 123 156 120 C 157 117 155 114 153 113 L 49 102 Z"
            fill="rgba(255,255,255,0.07)"/>
      {/* Left hand */}
      <ellipse cx="152" cy="118" rx="10" ry="7.5" fill={skin}/>
      <ellipse cx="152" cy="118" rx="10" ry="7.5" fill={`url(#sg${sid})`}/>
      <line x1="147" y1="116" x2="147" y2="120" stroke="rgba(0,0,0,0.15)" strokeWidth="0.8"/>
      <line x1="150" y1="115" x2="150" y2="121" stroke="rgba(0,0,0,0.15)" strokeWidth="0.8"/>
      <line x1="153" y1="115" x2="153" y2="121" stroke="rgba(0,0,0,0.15)" strokeWidth="0.8"/>

      {/* ── Collar ── */}
      {cStyle === 'v'
        ? <path d="M 87 91 L 100 107 L 113 91" fill="none" stroke={collar} strokeWidth="4.5" strokeLinecap="round" strokeLinejoin="round"/>
        : <path d="M 85 91 C 88 84 112 84 115 91" fill={collar}/>
      }

      {/* ── Neck ── */}
      <rect x="93" y="75" width="14" height="19" rx="5" fill={skin}/>
      <rect x="93" y="75" width="14" height="19" rx="5" fill={`url(#sg${sid})`}/>

      {/* ── Head ── */}
      <ellipse cx="100" cy="50" rx="29" ry="31" fill={skin}/>
      <ellipse cx="100" cy="50" rx="29" ry="31" fill={`url(#sg${sid})`}/>

      {/* Ears */}
      <ellipse cx="71"  cy="53" rx="5" ry="7" fill={skin}/>
      <ellipse cx="129" cy="53" rx="5" ry="7" fill={skin}/>
      <ellipse cx="73"  cy="53" rx="3" ry="5" fill="rgba(0,0,0,0.1)"/>
      <ellipse cx="127" cy="53" rx="3" ry="5" fill="rgba(0,0,0,0.1)"/>

      {/* ── Eyes ── */}
      {eStyle === 'normal' && <>
        <ellipse cx="90"  cy="51" rx="6.5" ry="5.5" fill="white"/>
        <ellipse cx="90"  cy="51" rx="4"   ry="4"   fill="#1e1008"/>
        <ellipse cx="91.8" cy="49.2" rx="1.5" ry="1.5" fill="white"/>
        <ellipse cx="110" cy="51" rx="6.5" ry="5.5" fill="white"/>
        <ellipse cx="110" cy="51" rx="4"   ry="4"   fill="#1e1008"/>
        <ellipse cx="111.8" cy="49.2" rx="1.5" ry="1.5" fill="white"/>
        <path d="M 83 44 C 86 41.5 93 42 96 44" stroke={hColor} strokeWidth="2.2" strokeLinecap="round" fill="none"/>
        <path d="M 104 44 C 107 42 114 41.5 117 44" stroke={hColor} strokeWidth="2.2" strokeLinecap="round" fill="none"/>
      </>}
      {eStyle === 'intense' && <>
        <ellipse cx="90"  cy="52" rx="6.5" ry="5" fill="white"/>
        <ellipse cx="90"  cy="52" rx="4"   ry="4" fill="#0d0600"/>
        <ellipse cx="91.8" cy="50.5" rx="1.5" ry="1.5" fill="white"/>
        <ellipse cx="110" cy="52" rx="6.5" ry="5" fill="white"/>
        <ellipse cx="110" cy="52" rx="4"   ry="4" fill="#0d0600"/>
        <ellipse cx="111.8" cy="50.5" rx="1.5" ry="1.5" fill="white"/>
        {/* Furrowed brows */}
        <path d="M 83 44.5 C 86 41 94 42 96 45" stroke={hColor} strokeWidth="2.8" strokeLinecap="round" fill="none"/>
        <path d="M 104 45 C 106 42 114 41 117 44.5" stroke={hColor} strokeWidth="2.8" strokeLinecap="round" fill="none"/>
        <line x1="89" y1="43.5" x2="91" y2="45.5" stroke={hColor} strokeWidth="1.2"/>
        <line x1="109" y1="45.5" x2="111" y2="43.5" stroke={hColor} strokeWidth="1.2"/>
      </>}
      {eStyle === 'relaxed' && <>
        <path d="M 83.5 51 C 87 47 93 47 96.5 51" fill="white"/>
        <ellipse cx="90" cy="51.5" rx="4" ry="3" fill="#1e1008"/>
        <path d="M 83.5 51 C 87 49.5 93 49.5 96.5 51" fill="none" stroke={skin} strokeWidth="2.8"/>
        <ellipse cx="91.5" cy="50.5" rx="1.2" ry="1" fill="white"/>
        <path d="M 103.5 51 C 107 47 113 47 116.5 51" fill="white"/>
        <ellipse cx="110" cy="51.5" rx="4" ry="3" fill="#1e1008"/>
        <path d="M 103.5 51 C 107 49.5 113 49.5 116.5 51" fill="none" stroke={skin} strokeWidth="2.8"/>
        <ellipse cx="111.5" cy="50.5" rx="1.2" ry="1" fill="white"/>
        <path d="M 83 44 C 87 43 93 43.5 96 45" stroke={hColor} strokeWidth="1.8" strokeLinecap="round" fill="none"/>
        <path d="M 104 45 C 107 43.5 113 43 117 44" stroke={hColor} strokeWidth="1.8" strokeLinecap="round" fill="none"/>
      </>}

      {/* ── Nose ── */}
      <path d="M 98 56 C 95 61 96 65 100 65 C 104 65 105 61 102 56"
            fill="none" stroke="rgba(0,0,0,0.18)" strokeWidth="1.3" strokeLinecap="round"/>

      {/* ── Mouth ── */}
      <path d="M 93 70 C 97 73.5 103 73.5 107 70"
            fill="none" stroke="rgba(0,0,0,0.28)" strokeWidth="1.6" strokeLinecap="round"/>

      {/* ── Beard ── */}
      {beard === 'stubble' && (
        <ellipse cx="100" cy="67" rx="18" ry="12" fill={hColor} opacity="0.18"/>
      )}
      {beard === 'goatee' && <>
        <ellipse cx="100" cy="65" rx="9" ry="5" fill={hColor} opacity="0.45"/>
        <path d="M 91 68 C 95 76 105 76 109 68" fill={hColor} opacity="0.45"/>
        {/* Moustache */}
        <path d="M 93 65 C 96 63 104 63 107 65" fill={hColor} opacity="0.4"/>
      </>}
      {beard === 'full' && (
        <path d="M 72 57 C 71 65 73 73 78 77 C 85 82 115 82 122 77 C 127 73 129 65 128 57"
              fill={hColor} opacity="0.5"/>
      )}

      {/* ── Hair ── */}
      {hStyle === 'low' && <>
        <ellipse cx="100" cy="24" rx="29" ry="17" fill={hColor}/>
        <path d="M 71 51 C 72 42 77 36 83 33 C 90 30 96 29 100 29 C 104 29 110 30 117 33 C 123 36 128 42 129 51"
              fill={hColor}/>
        <ellipse cx="85" cy="24" rx="9" ry="5" fill="rgba(255,255,255,0.07)" transform="rotate(-10 85 24)"/>
      </>}
      {hStyle === 'crop' && <>
        <ellipse cx="100" cy="25" rx="28" ry="16" fill={hColor}/>
        <path d="M 72 51 C 74 42 80 35 100 32 C 120 35 126 42 128 51" fill={hColor}/>
        <ellipse cx="84" cy="23" rx="10" ry="5" fill="rgba(255,255,255,0.08)" transform="rotate(-12 84 23)"/>
        {/* Texture lines */}
        <path d="M 76 36 C 82 33 95 32 108 33 C 118 35 124 38 127 42"
              fill="none" stroke="rgba(255,255,255,0.06)" strokeWidth="1.5"/>
      </>}
      {hStyle === 'afro' && <>
        <ellipse cx="100" cy="26" rx="39" ry="34" fill={hColor}/>
        <ellipse cx="100" cy="30" rx="37" ry="28" fill="rgba(0,0,0,0.12)"/>
        {/* Afro puff highlights */}
        <ellipse cx="80"  cy="14" rx="10" ry="9" fill="rgba(255,255,255,0.05)"/>
        <ellipse cx="116" cy="12" rx="9"  ry="8" fill="rgba(255,255,255,0.05)"/>
        <ellipse cx="100" cy="9"  rx="8"  ry="7" fill="rgba(255,255,255,0.05)"/>
        <ellipse cx="70"  cy="28" rx="7"  ry="10" fill="rgba(255,255,255,0.04)"/>
        <ellipse cx="130" cy="28" rx="7"  ry="10" fill="rgba(255,255,255,0.04)"/>
        <path d="M 71 51 C 73 40 78 33 86 29 C 92 26 97 25 100 25 C 103 25 108 26 114 29 C 122 33 127 40 129 51"
              fill={hColor}/>
      </>}
      {hStyle === 'mohawk' && <>
        {/* Side fade */}
        <path d="M 71 40 C 72 32 77 25 84 21 L 84 51 C 78 51 72 47 71 40 Z" fill={hColor} opacity="0.55"/>
        <path d="M 129 40 C 128 32 123 25 116 21 L 116 51 C 122 51 128 47 129 40 Z" fill={hColor} opacity="0.55"/>
        {/* Mohawk strip */}
        <path d="M 86 51 C 86 40 92 17 100 7 C 108 17 114 40 114 51 Z" fill={hColor}/>
        <path d="M 89 51 C 89 42 94 21 100 11 C 106 21 111 42 111 51 Z" fill="rgba(255,255,255,0.08)"/>
      </>}
      {hStyle === 'waves' && <>
        <ellipse cx="100" cy="25" rx="29" ry="16" fill={hColor}/>
        <path d="M 71 51 C 73 42 78 35 100 32 C 122 35 127 42 129 51" fill={hColor}/>
        {/* Wave lines */}
        <path d="M 75 40 C 80 37 87 39 92 37 C 97 35 103 37 108 35 C 114 33 120 35 125 38"
              fill="none" stroke="rgba(255,255,255,0.13)" strokeWidth="1.8" strokeLinecap="round"/>
        <path d="M 74 45 C 80 42 87 44 92 42 C 97 40 103 42 109 40 C 114 38 120 40 126 43"
              fill="none" stroke="rgba(255,255,255,0.10)" strokeWidth="1.6" strokeLinecap="round"/>
      </>}
      {hStyle === 'bald' && (
        <ellipse cx="88" cy="27" rx="13" ry="6" fill="rgba(255,255,255,0.05)" transform="rotate(-18 88 27)"/>
      )}

      {/* ── Name plate ── */}
      {playerName && (
        <text x="100" y="256"
              textAnchor="middle"
              fontFamily="'Arial Black',Arial,sans-serif"
              fontWeight="900" fontSize="10"
              fill="rgba(255,255,255,0.75)"
              letterSpacing="2">
          {playerName.toUpperCase().slice(0, 12)}
        </text>
      )}
    </svg>
  );
}

// ── Section header ─────────────────────────────────────────────────────────────
function Section({ label, children }) {
  return (
    <div className="mb-5">
      <p className="text-[10px] font-black uppercase tracking-[0.18em] text-slate-400 mb-2">{label}</p>
      {children}
    </div>
  );
}

// ── Main page ──────────────────────────────────────────────────────────────────
export default function AvatarBuilder() {
  const navigate = useNavigate();
  const saved    = getSavedAvatar();
  const jersey   = getSavedJersey();
  const { player } = (() => {
    try { return JSON.parse(localStorage.getItem('eko_player_auth') || '{}'); } catch { return {}; }
  })();
  const pName   = player?.baller_name || player?.first_name || '';
  const pNumber = jersey?.playerNumber || '';

  const [cfg, setCfg] = useState({
    skinTone:  saved?.skinTone  || 'tone3',
    hairStyle: saved?.hairStyle || 'low',
    hairColor: saved?.hairColor || 'black',
    eyeStyle:  saved?.eyeStyle  || 'normal',
    beard:     saved?.beard     || 'none',
    ...saved,
  });
  const [savedMsg, setSavedMsg] = useState(false);

  const set = (key, val) => setCfg(c => ({ ...c, [key]: val }));

  const handleSave = () => {
    localStorage.setItem(AVATAR_KEY, JSON.stringify(cfg));
    setSavedMsg(true);
    setTimeout(() => setSavedMsg(false), 2200);
  };

  const skinColor  = SKIN_TONES.find(t => t.id === cfg.skinTone)?.color || '#C68642';
  const hairColor  = HAIR_COLORS.find(h => h.id === cfg.hairColor)?.color || '#1a1010';
  const avatarCfg  = { ...cfg, skinTone: skinColor, hairColor };

  const pill = (active) =>
    `px-3 py-1.5 rounded-lg text-xs font-bold transition-all border ${
      active
        ? 'bg-primary text-white border-primary shadow-sm shadow-primary/30'
        : 'bg-white/5 text-slate-400 border-white/10 hover:border-primary/40 hover:text-white'
    }`;

  return (
    <div className="bg-background-light dark:bg-background-dark text-slate-900 dark:text-slate-100 min-h-screen flex flex-col font-display">
      {/* Header */}
      <header className="flex items-center justify-between px-4 py-4 border-b border-slate-200 dark:border-slate-800 sticky top-0 z-10 bg-background-light dark:bg-background-dark">
        <button onClick={() => navigate('/dashboard')} className="p-2 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-full">
          <span className="material-symbols-outlined">arrow_back</span>
        </button>
        <div className="text-center">
          <h1 className="text-base font-black text-white">My Avatar</h1>
          <p className="text-[10px] text-slate-500">Build your player card</p>
        </div>
        <button
          onClick={handleSave}
          className={`px-3 py-1.5 rounded-lg text-sm font-bold transition-all ${savedMsg ? 'bg-primary/20 text-primary' : 'bg-primary text-white'}`}
        >
          {savedMsg ? '✓ Saved' : 'Save'}
        </button>
      </header>

      <main className="flex-1 overflow-y-auto pb-10">
        {/* Avatar preview */}
        <div
          className="flex items-center justify-center py-6 relative overflow-hidden"
          style={{
            background: `radial-gradient(circle at 50% 60%, ${jersey?.teamColor || '#0ac247'}25 0%, transparent 65%), #080d09`,
            minHeight: 300,
          }}
        >
          {/* Glow */}
          <div
            className="absolute size-64 rounded-full blur-3xl opacity-20 pointer-events-none"
            style={{ backgroundColor: jersey?.teamColor || '#0ac247' }}
          />
          <div className="relative z-10">
            <AvatarSVG
              config={avatarCfg}
              kit={jersey?.kit}
              playerName={pName}
              playerNumber={pNumber}
              size={210}
            />
          </div>
          {/* Jersey badge */}
          {jersey?.teamName && (
            <div className="absolute bottom-3 left-1/2 -translate-x-1/2 flex items-center gap-1.5 px-3 py-1 rounded-full bg-black/50 border border-white/10 backdrop-blur-sm">
              <span className="material-symbols-outlined text-primary text-[13px]">checkroom</span>
              <span className="text-[10px] font-bold text-white/80">{jersey.teamName} · {jersey.kitType} Kit</span>
            </div>
          )}
        </div>

        {/* Options */}
        <div className="px-4 pt-5">

          {/* Skin Tone */}
          <Section label="Skin Tone">
            <div className="flex gap-2.5 flex-wrap">
              {SKIN_TONES.map(t => (
                <button
                  key={t.id}
                  title={t.label}
                  onClick={() => set('skinTone', t.id)}
                  className={`size-9 rounded-full border-2 transition-all ${cfg.skinTone === t.id ? 'border-primary scale-110 shadow-md shadow-primary/30' : 'border-transparent hover:scale-105'}`}
                  style={{ backgroundColor: t.color }}
                />
              ))}
            </div>
          </Section>

          {/* Hair Style */}
          <Section label="Hair Style">
            <div className="flex flex-wrap gap-2">
              {HAIR_STYLES.map(h => (
                <button key={h.id} onClick={() => set('hairStyle', h.id)} className={pill(cfg.hairStyle === h.id)}>
                  {h.label}
                </button>
              ))}
            </div>
          </Section>

          {/* Hair Color (hidden for bald) */}
          {cfg.hairStyle !== 'bald' && (
            <Section label="Hair Color">
              <div className="flex gap-2.5">
                {HAIR_COLORS.map(h => (
                  <button
                    key={h.id}
                    title={h.id}
                    onClick={() => set('hairColor', h.id)}
                    className={`size-9 rounded-full border-2 transition-all ${cfg.hairColor === h.id ? 'border-primary scale-110 shadow-md shadow-primary/30' : 'border-transparent hover:scale-105'}`}
                    style={{ backgroundColor: h.color }}
                  />
                ))}
              </div>
            </Section>
          )}

          {/* Eye Style */}
          <Section label="Eyes">
            <div className="flex flex-wrap gap-2">
              {EYE_STYLES.map(e => (
                <button key={e.id} onClick={() => set('eyeStyle', e.id)} className={pill(cfg.eyeStyle === e.id)}>
                  {e.label}
                </button>
              ))}
            </div>
          </Section>

          {/* Beard */}
          <Section label="Beard / Facial Hair">
            <div className="flex flex-wrap gap-2">
              {BEARDS.map(b => (
                <button key={b.id} onClick={() => set('beard', b.id)} className={pill(cfg.beard === b.id)}>
                  {b.label}
                </button>
              ))}
            </div>
          </Section>

          {/* Jersey note */}
          <div className="mt-2 p-3 rounded-xl bg-primary/8 border border-primary/20 flex gap-2.5 items-start">
            <span className="material-symbols-outlined text-primary text-[18px] shrink-0 mt-0.5">info</span>
            <p className="text-xs text-slate-400 leading-relaxed">
              Your avatar automatically wears your saved jersey.{' '}
              <button onClick={() => navigate('/jersey')} className="text-primary font-bold hover:underline">
                Change jersey →
              </button>
            </p>
          </div>

        </div>
      </main>
    </div>
  );
}
