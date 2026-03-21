import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { getSavedJersey } from './JerseyDesigner';
import { generateMemberAvatar, getMemberAvatarStatus, lockMemberAvatar } from '../api';
import { getPlayerAuth } from './Login';

// ── Storage ────────────────────────────────────────────────────────────────────
const AVATAR_KEY = 'eko_avatar';

export function getSavedAvatar() {
  try { return JSON.parse(localStorage.getItem(AVATAR_KEY)); } catch { return null; }
}

// ── DiceBear fallback URL (shown before AI image is generated) ─────────────────
export function buildFallbackUrl(config, jerseyColor) {
  const {
    seed      = 'player',
    skinTone  = 'edb98a',
    hairStyle = 'short1',
    face      = 'smile',
    accessory = 'none',
  } = config || {};

  const clothing = (jerseyColor || '#0ac247').replace('#', '');
  const params = new URLSearchParams();
  params.set('seed', seed);
  params.set('skinColor[]', skinTone);
  params.set('head[]', hairStyle);
  params.set('face[]', face);
  params.set('clothingColor[]', clothing);
  if (accessory !== 'none') params.set('accessories[]', accessory);
  params.set('backgroundColor', 'transparent');
  return `https://api.dicebear.com/9.x/open-peeps/svg?${params.toString()}`;
}

// ── Exported avatar component used on Dashboard ────────────────────────────────
export function AvatarSVG({ config, kit, playerName, size = 240 }) {
  const saved = getSavedAvatar();
  // If AI image saved, use it; otherwise fall back to DiceBear
  const aiImage = saved?.aiImage;
  const jerseyColor = kit?.main;
  const seed = playerName || 'player';

  if (aiImage) {
    return (
      <img
        src={aiImage}
        alt="Player avatar"
        width={size}
        height={size}
        style={{ objectFit: 'contain', objectPosition: 'bottom' }}
      />
    );
  }
  const url = buildFallbackUrl({ ...config, seed }, jerseyColor);
  return (
    <img src={url} alt="Player avatar" width={size} height={size}
      style={{ objectFit: 'contain' }} />
  );
}

// ── Options ────────────────────────────────────────────────────────────────────
export const SKIN_TONES = [
  { id: 'ffdbb4', color: '#ffdbb4', label: 'Light' },
  { id: 'edb98a', color: '#edb98a', label: 'Medium Light' },
  { id: 'd08b5b', color: '#d08b5b', label: 'Medium' },
  { id: 'ae5d29', color: '#ae5d29', label: 'Medium Dark' },
  { id: '694d3d', color: '#694d3d', label: 'Dark' },
];
export const HAIR_COLORS = []; // kept for Dashboard compat import

const HAIR_OPTIONS = [
  { id: 'noHair1',    label: 'Bald' },
  { id: 'shaved1',    label: 'Fade' },
  { id: 'short1',     label: 'Short' },
  { id: 'short3',     label: 'Waves' },
  { id: 'afro',       label: 'Afro' },
  { id: 'longAfro',   label: 'Long Afro' },
  { id: 'mohawk',     label: 'Mohawk' },
  { id: 'flatTop',    label: 'Flat Top' },
  { id: 'cornrows',   label: 'Cornrows' },
  { id: 'dreads1',    label: 'Dreads' },
  { id: 'twists',     label: 'Twists' },
  { id: 'bantuKnots', label: 'Bantu Knots' },
  { id: 'bun',        label: 'Bun' },
  { id: 'medium1',    label: 'Medium' },
  { id: 'long',       label: 'Long' },
];
const FACE_OPTIONS = [
  { id: 'smile',      label: 'Smile' },
  { id: 'smileBig',   label: 'Big Smile' },
  { id: 'calm',       label: 'Calm' },
  { id: 'serious',    label: 'Serious' },
  { id: 'driven',     label: 'Focused' },
  { id: 'cheeky',     label: 'Cheeky' },
  { id: 'solemn',     label: 'Solemn' },
  { id: 'suspicious', label: 'Sly' },
];
const ACCESSORY_OPTIONS = [
  { id: 'none',        label: 'None' },
  { id: 'glasses',     label: 'Glasses' },
  { id: 'glasses2',    label: 'Glasses 2' },
  { id: 'sunglasses',  label: 'Sunglasses' },
  { id: 'sunglasses2', label: 'Shades' },
];
const BEARD_OPTIONS = [
  { id: 'none',        label: 'None' },
  { id: 'stubble',     label: 'Stubble' },
  { id: 'goatee',      label: 'Goatee' },
  { id: 'chinstrap',   label: 'Chinstrap' },
  { id: 'full',        label: 'Full Beard' },
  { id: 'thick',       label: 'Thick Beard' },
  { id: 'long_beard',  label: 'Long Beard' },
];
const TATTOO_OPTIONS = [
  { id: 'none',         label: 'None' },
  { id: 'left_sleeve',  label: 'Left Arm Sleeve' },
  { id: 'both_sleeves', label: 'Both Arms' },
  { id: 'neck',         label: 'Neck Tattoo' },
  { id: 'chest',        label: 'Chest Visible' },
  { id: 'hand',         label: 'Hand Tattoo' },
];

// ── UI helpers ─────────────────────────────────────────────────────────────────
function Pill({ label, active, onClick }) {
  return (
    <button onClick={onClick}
      className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-all border ${
        active
          ? 'bg-primary text-white border-primary shadow-sm shadow-primary/30'
          : 'bg-white/5 text-slate-400 border-white/10 hover:border-primary/40 hover:text-white'
      }`}>
      {label}
    </button>
  );
}
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
  const navigate    = useNavigate();
  const jersey      = getSavedJersey();
  const saved       = getSavedAvatar();
  const { token, player } = getPlayerAuth();

  const seed        = player?.baller_name || player?.first_name || 'player';
  const jerseyNum   = jersey?.playerNumber || '';
  const teamName    = jersey?.teamName || '';
  const jerseyColor = jersey?.teamColor || '#0ac247';

  const [cfg, setCfg] = useState({
    skinTone:  saved?.skinTone  || 'edb98a',
    hairStyle: saved?.hairStyle || 'short1',
    face:      saved?.face      || 'smile',
    accessory: saved?.accessory || 'none',
    beard:     saved?.beard     || 'none',
    tattoo:    saved?.tattoo    || 'none',
  });

  const [aiImage,       setAiImage]       = useState(saved?.aiImage || null);
  const [generating,    setGenerating]    = useState(false);
  const [genError,      setGenError]      = useState('');
  const [savedMsg,      setSavedMsg]      = useState(false);
  const [avatarAccess,  setAvatarAccess]  = useState(null); // null = loading
  const [avatarLocked,  setAvatarLocked]  = useState(false);

  // Check access + lock status on mount
  useEffect(() => {
    if (!token) return;
    getMemberAvatarStatus(token)
      .then(d => { setAvatarAccess(d.avatar_access); setAvatarLocked(d.avatar_locked); })
      .catch(() => { setAvatarAccess(false); });
  }, [token]);

  const hasJersey = !!jersey?.teamId;

  const set = (key, val) => {
    setCfg(c => ({ ...c, [key]: val }));
    setAiImage(null);
  };

  const handleGenerate = async () => {
    setGenerating(true);
    setGenError('');
    try {
      const result = await generateMemberAvatar(token, {
        skin_tone:     cfg.skinTone,
        hair_style:    cfg.hairStyle,
        face:          cfg.face,
        accessory:     cfg.accessory,
        beard:         cfg.beard,
        tattoo:        cfg.tattoo,
        player_name:   seed,
        jersey_number: jerseyNum,
        team_name:     teamName,
        jersey_color:  jerseyColor,
      });

      let imgSrc = null;
      if (result.image_b64) {
        imgSrc = `data:image/png;base64,${result.image_b64}`;
      } else if (result.image_url) {
        imgSrc = result.image_url;
      }

      if (imgSrc) {
        setAiImage(imgSrc);
      } else {
        setGenError('No image returned. Try again.');
      }
    } catch (err) {
      const msg = err.response?.data?.detail || 'Generation failed. Try again.';
      setGenError(msg);
    } finally {
      setGenerating(false);
    }
  };

  const handleSave = async () => {
    localStorage.setItem(AVATAR_KEY, JSON.stringify({ ...cfg, seed, aiImage }));
    // Lock server-side and persist the URL so other devices can load it
    try { await lockMemberAvatar(token, aiImage || ''); setAvatarLocked(true); } catch { /* non-fatal */ }
    setSavedMsg(true);
    setTimeout(() => setSavedMsg(false), 2200);
  };

  const fallbackUrl = buildFallbackUrl({ ...cfg, seed }, jerseyColor);
  const displayImg  = aiImage || fallbackUrl;
  const isAI        = !!aiImage;

  // ── Gate screens ────────────────────────────────────────────────────────────
  const GateScreen = ({ icon, title, body, action }) => (
    <div className="bg-background-light dark:bg-background-dark min-h-screen flex flex-col font-display">
      <header className="flex items-center px-4 py-4 border-b border-slate-200 dark:border-slate-800">
        <button onClick={() => navigate('/dashboard')} className="p-2 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-full">
          <span className="material-symbols-outlined">arrow_back</span>
        </button>
        <h1 className="ml-3 text-base font-black text-white">My Avatar</h1>
      </header>
      <div className="flex-1 flex flex-col items-center justify-center gap-4 px-8 text-center">
        <span className="material-symbols-outlined text-primary" style={{ fontSize: 64 }}>{icon}</span>
        <h2 className="text-lg font-black text-white">{title}</h2>
        <p className="text-sm text-slate-400 leading-relaxed max-w-xs">{body}</p>
        {action}
      </div>
    </div>
  );

  if (avatarAccess === null) {
    return <GateScreen icon="hourglass_empty" title="Checking access…" body="Just a moment." />;
  }

  if (!hasJersey) {
    return (
      <GateScreen
        icon="checkroom"
        title="Set your jersey first"
        body="You need to pick and save a club jersey before you can create your avatar. Your avatar will automatically wear your jersey colours."
        action={
          <button onClick={() => navigate('/jersey')}
            className="mt-2 px-5 py-2.5 bg-primary text-white font-bold rounded-xl text-sm hover:bg-primary/90">
            Go to My Jersey →
          </button>
        }
      />
    );
  }

  if (!avatarAccess) {
    return (
      <GateScreen
        icon="lock"
        title="Avatar access required"
        body="Avatar creation is currently invite-only. Contact your admin to request access — they can unlock it for you in seconds."
      />
    );
  }

  if (avatarLocked) {
    return (
      <GateScreen
        icon="verified"
        title="Avatar saved & locked"
        body={`Your player card is set, ${seed}. To change it, contact your admin and ask them to reset your avatar. This keeps the credits fair for everyone.`}
        action={
          <div className="mt-2 rounded-xl overflow-hidden border-2 border-primary/40 flex flex-col items-center"
            style={{ width: 200, height: 260, background: `linear-gradient(160deg, ${jerseyColor}30 0%, #0d1117 55%)` }}>
            <img src={aiImage || buildFallbackUrl({ ...cfg, seed }, jerseyColor)}
              alt="avatar" className="w-full h-full object-contain object-bottom" />
          </div>
        }
      />
    );
  }

  return (
    <div className="bg-background-light dark:bg-background-dark text-slate-900 dark:text-slate-100 min-h-screen flex flex-col font-display">
      {/* Header */}
      <header className="flex items-center justify-between px-4 py-4 border-b border-slate-200 dark:border-slate-800 sticky top-0 z-10 bg-background-light dark:bg-background-dark">
        <button onClick={() => navigate('/dashboard')}
          className="p-2 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-full">
          <span className="material-symbols-outlined">arrow_back</span>
        </button>
        <div className="text-center">
          <h1 className="text-base font-black text-white">My Avatar</h1>
          <p className="text-[10px] text-slate-500">AI Player Card</p>
        </div>
        <button onClick={handleSave}
          disabled={!aiImage}
          className={`px-3 py-1.5 rounded-lg text-sm font-bold transition-all ${
            savedMsg
              ? 'bg-primary/20 text-primary'
              : aiImage
                ? 'bg-primary text-white'
                : 'bg-white/10 text-slate-600 cursor-not-allowed'
          }`}>
          {savedMsg ? '✓ Saved' : 'Save'}
        </button>
      </header>

      <main className="flex-1 overflow-y-auto pb-10">
        {/* ── Player Card Preview ── */}
        <div
          className="flex flex-col items-center justify-center py-8 relative overflow-hidden"
          style={{
            background: `radial-gradient(circle at 50% 60%, ${jerseyColor}28 0%, transparent 65%), #080d09`,
            minHeight: 380,
          }}
        >
          {/* Ambient glow */}
          <div className="absolute size-80 rounded-full blur-3xl opacity-15 pointer-events-none"
            style={{ backgroundColor: jerseyColor }} />

          {/* Card */}
          <div className="relative z-10 flex flex-col items-center">
            <div
              className="relative rounded-2xl overflow-hidden flex flex-col"
              style={{
                width: 230,
                height: 300,
                background: `linear-gradient(160deg, ${jerseyColor}30 0%, #0d1117 55%)`,
                border: `2px solid ${jerseyColor}`,
                boxShadow: `0 0 50px ${jerseyColor}50, 0 24px 60px rgba(0,0,0,0.7)`,
              }}
            >
              {/* Top colour bar */}
              <div className="h-1 w-full shrink-0" style={{ background: jerseyColor }} />

              {/* AI badge or fallback label */}
              <div className="absolute top-3 left-3 z-20">
                {isAI ? (
                  <span className="flex items-center gap-1 text-[9px] font-black uppercase tracking-widest text-white bg-primary/80 px-2 py-0.5 rounded-full">
                    <span className="material-symbols-outlined text-[10px]">auto_awesome</span> AI
                  </span>
                ) : (
                  <span className="text-[9px] font-bold text-white/30 uppercase tracking-widest">Preview</span>
                )}
              </div>

              {/* Avatar image */}
              {generating ? (
                <div className="flex-1 flex flex-col items-center justify-center gap-3">
                  <div className="size-12 rounded-full border-4 border-slate-700 border-t-primary animate-spin" />
                  <p className="text-xs text-slate-400 text-center px-4">Generating your<br/>player card…</p>
                  <p className="text-[9px] text-slate-600">This takes ~15 seconds</p>
                </div>
              ) : (
                <img
                  src={displayImg}
                  alt="avatar"
                  className="absolute bottom-12 left-1/2 -translate-x-1/2"
                  style={{ width: 220, height: 260, objectFit: 'contain', objectPosition: 'bottom' }}
                />
              )}

              {/* Name plate */}
              {!generating && (
                <div className="absolute bottom-0 inset-x-0 px-3 pb-2.5 z-10">
                  <div className="rounded-xl px-3 py-2 text-center"
                    style={{ background: 'rgba(0,0,0,0.75)', backdropFilter: 'blur(6px)' }}>
                    <p className="text-white font-black text-sm tracking-widest uppercase leading-none">
                      {seed}
                    </p>
                    <p className="text-[9px] font-bold mt-0.5" style={{ color: jerseyColor }}>
                      {teamName || 'Eko Football'}
                      {jerseyNum ? ` · #${jerseyNum}` : ''}
                    </p>
                  </div>
                </div>
              )}
            </div>

            {/* Generate / Regenerate button */}
            <button
              onClick={handleGenerate}
              disabled={generating}
              className={`mt-4 flex items-center gap-2 px-5 py-2.5 rounded-xl font-bold text-sm transition-all ${
                generating
                  ? 'bg-white/5 text-slate-600 cursor-not-allowed'
                  : 'bg-primary text-white hover:bg-primary/90 shadow-lg shadow-primary/30 active:scale-95'
              }`}
            >
              <span className="material-symbols-outlined text-[18px]">
                {generating ? 'hourglass_empty' : isAI ? 'refresh' : 'auto_awesome'}
              </span>
              {generating ? 'Generating…' : isAI ? 'Regenerate' : 'Generate AI Avatar'}
            </button>

            {genError && (
              <p className="mt-2 text-xs text-red-400 text-center max-w-[220px]">{genError}</p>
            )}

            {!isAI && !generating && (
              <p className="mt-2 text-[10px] text-slate-500 text-center max-w-[220px]">
                Configure your look below, then tap Generate
              </p>
            )}

            {isAI && !savedMsg && (
              <p className="mt-1 text-[10px] text-slate-500">Happy with it? Hit Save ↗</p>
            )}
          </div>
        </div>

        {/* ── Options ── */}
        <div className="px-4 pt-5">

          <Section label="Skin Tone">
            <div className="flex gap-3 flex-wrap">
              {SKIN_TONES.map(t => (
                <button key={t.id} title={t.label} onClick={() => set('skinTone', t.id)}
                  className={`size-10 rounded-full border-2 transition-all ${
                    cfg.skinTone === t.id
                      ? 'border-primary scale-110 shadow-md shadow-primary/30'
                      : 'border-transparent hover:scale-105'
                  }`}
                  style={{ backgroundColor: t.color }} />
              ))}
            </div>
          </Section>

          <Section label="Hair Style">
            <div className="flex flex-wrap gap-2">
              {HAIR_OPTIONS.map(h => (
                <Pill key={h.id} label={h.label} active={cfg.hairStyle === h.id} onClick={() => set('hairStyle', h.id)} />
              ))}
            </div>
          </Section>

          <Section label="Expression">
            <div className="flex flex-wrap gap-2">
              {FACE_OPTIONS.map(f => (
                <Pill key={f.id} label={f.label} active={cfg.face === f.id} onClick={() => set('face', f.id)} />
              ))}
            </div>
          </Section>

          <Section label="Accessories">
            <div className="flex flex-wrap gap-2">
              {ACCESSORY_OPTIONS.map(a => (
                <Pill key={a.id} label={a.label} active={cfg.accessory === a.id} onClick={() => set('accessory', a.id)} />
              ))}
            </div>
          </Section>

          <Section label="Beard / Facial Hair">
            <div className="flex flex-wrap gap-2">
              {BEARD_OPTIONS.map(b => (
                <Pill key={b.id} label={b.label} active={cfg.beard === b.id} onClick={() => set('beard', b.id)} />
              ))}
            </div>
          </Section>

          <Section label="Tattoos">
            <div className="flex flex-wrap gap-2">
              {TATTOO_OPTIONS.map(t => (
                <Pill key={t.id} label={t.label} active={cfg.tattoo === t.id} onClick={() => set('tattoo', t.id)} />
              ))}
            </div>
          </Section>

          <div className="mt-2 p-3 rounded-xl bg-primary/8 border border-primary/20 flex gap-2.5 items-start">
            <span className="material-symbols-outlined text-primary text-[18px] shrink-0 mt-0.5">info</span>
            <p className="text-xs text-slate-400 leading-relaxed">
              Avatar wears your saved jersey colours.{' '}
              <button onClick={() => navigate('/jersey')} className="text-primary font-bold hover:underline">
                Change jersey →
              </button>
              {' '}Changing options clears the AI image — hit Generate again for a new one.
            </p>
          </div>

        </div>
      </main>
    </div>
  );
}
