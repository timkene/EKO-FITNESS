import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { getPlayerAuth, setPlayerAuth, clearPlayerAuth } from './Login';
import { getMemberProfile, updateMemberProfile, changeMemberPassword, invalidateStatsCache } from '../api';
import JerseyAvatar from '../components/JerseyAvatar';

// Password must be letters, digits, hyphens or underscores only
const PASSWORD_PATTERN = /^[A-Za-z0-9\-_]+$/;

function PasswordStrengthBar({ password }) {
  if (!password) return null;
  let score = 0;
  if (password.length >= 8) score++;
  if (password.length >= 12) score++;
  if (/[A-Z]/.test(password)) score++;
  if (/[a-z]/.test(password)) score++;
  if (/[0-9]/.test(password)) score++;

  const labels = ['Too short', 'Weak', 'Fair', 'Good', 'Strong', 'Very strong'];
  const colors = ['bg-red-500', 'bg-orange-500', 'bg-yellow-500', 'bg-lime-500', 'bg-green-500', 'bg-emerald-500'];
  const textColors = ['text-red-400', 'text-orange-400', 'text-yellow-400', 'text-lime-400', 'text-green-400', 'text-emerald-400'];

  return (
    <div className="mt-2 space-y-1">
      <div className="flex gap-1">
        {[1, 2, 3, 4, 5].map((i) => (
          <div
            key={i}
            className={`h-1 flex-1 rounded-full transition-colors duration-200 ${i <= score ? colors[score] : 'bg-slate-700'}`}
          />
        ))}
      </div>
      <p className={`text-xs ${textColors[score]}`}>{labels[score]}</p>
    </div>
  );
}

const inputCls =
  'w-full rounded-lg bg-slate-800/60 border border-slate-700 px-3 py-2.5 text-sm text-slate-100 placeholder-slate-500 focus:outline-none focus:border-primary/60 focus:ring-1 focus:ring-primary/30 transition-colors';
const labelCls = 'block text-xs font-semibold text-slate-400 mb-1.5 uppercase tracking-wider';

function ShowHideButton({ show, onToggle }) {
  return (
    <button
      type="button"
      onClick={onToggle}
      tabIndex={-1}
      aria-label={show ? 'Hide password' : 'Show password'}
      className="absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-400 hover:text-primary transition-colors"
    >
      <span className="material-symbols-outlined text-lg">{show ? 'visibility_off' : 'visibility'}</span>
    </button>
  );
}

function StatusBanner({ msg }) {
  if (!msg?.text) return null;
  const isSuccess = msg.type === 'success';
  return (
    <div
      className={`text-sm px-3 py-2.5 rounded-lg flex items-start gap-2 ${
        isSuccess
          ? 'bg-green-500/10 text-green-400 border border-green-500/20'
          : 'bg-red-500/10 text-red-400 border border-red-500/20'
      }`}
    >
      <span className="material-symbols-outlined text-base shrink-0 mt-0.5">
        {isSuccess ? 'check_circle' : 'error'}
      </span>
      {msg.text}
    </div>
  );
}

export default function Profile() {
  const navigate = useNavigate();
  const { token, player } = getPlayerAuth();

  const [profile, setProfile] = useState(null);
  const [loading, setLoading] = useState(true);

  // ── Edit details state ──────────────────────────────────────────────────────
  const [ballerName, setBallerName] = useState('');
  const [jerseyNumber, setJerseyNumber] = useState('');
  const [whatsappPhone, setWhatsappPhone] = useState('');
  const [detailsMsg, setDetailsMsg] = useState({ type: '', text: '' });
  const [savingDetails, setSavingDetails] = useState(false);
  const [detailsDirty, setDetailsDirty] = useState(false);

  // ── Change password state ───────────────────────────────────────────────────
  const [currentPw, setCurrentPw] = useState('');
  const [newPw, setNewPw] = useState('');
  const [confirmPw, setConfirmPw] = useState('');
  const [showCurrentPw, setShowCurrentPw] = useState(false);
  const [showNewPw, setShowNewPw] = useState(false);
  const [showConfirmPw, setShowConfirmPw] = useState(false);
  const [passwordMsg, setPasswordMsg] = useState({ type: '', text: '' });
  const [savingPassword, setSavingPassword] = useState(false);
  const [loggingOut, setLoggingOut] = useState(false);

  useEffect(() => {
    if (!token) { navigate('/login', { replace: true }); return; }
    setLoading(true);
    getMemberProfile(token)
      .then((d) => {
        setProfile(d.player);
        setBallerName(d.player.baller_name || '');
        setJerseyNumber(String(d.player.jersey_number ?? ''));
        setWhatsappPhone(d.player.whatsapp_phone || '');
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [token, navigate]);

  // Track whether any detail field changed from the loaded profile
  useEffect(() => {
    if (!profile) return;
    const changed =
      ballerName.trim() !== (profile.baller_name || '') ||
      String(jerseyNumber) !== String(profile.jersey_number ?? '') ||
      whatsappPhone.trim() !== (profile.whatsapp_phone || '');
    setDetailsDirty(changed);
  }, [ballerName, jerseyNumber, whatsappPhone, profile]);

  const handleSaveDetails = async (e) => {
    e.preventDefault();
    setDetailsMsg({ type: '', text: '' });
    const name = ballerName.trim();
    const num = parseInt(jerseyNumber, 10);
    const phone = whatsappPhone.trim();
    if (!name) return setDetailsMsg({ type: 'error', text: 'Baller name cannot be empty.' });
    if (!num || num < 1 || num > 100) return setDetailsMsg({ type: 'error', text: 'Shirt number must be between 1 and 100.' });
    if (!phone) return setDetailsMsg({ type: 'error', text: 'WhatsApp number cannot be empty.' });
    setSavingDetails(true);
    try {
      const res = await updateMemberProfile(token, { baller_name: name, jersey_number: num, whatsapp_phone: phone });
      // Reflect changes in the sidebar immediately by updating localStorage
      const updated = { ...player, baller_name: res.baller_name || name, jersey_number: res.jersey_number ?? num };
      setPlayerAuth(token, updated);
      setProfile((p) => ({ ...p, baller_name: res.baller_name || name, jersey_number: res.jersey_number ?? num, whatsapp_phone: phone }));
      invalidateStatsCache();
      setDetailsDirty(false);
      setDetailsMsg({ type: 'success', text: 'Profile updated! Your new details are saved.' });
    } catch (err) {
      setDetailsMsg({ type: 'error', text: err.response?.data?.detail || 'Could not update profile. Try again.' });
    } finally {
      setSavingDetails(false);
    }
  };

  const handleChangePassword = async (e) => {
    e.preventDefault();
    setPasswordMsg({ type: '', text: '' });
    if (!currentPw) return setPasswordMsg({ type: 'error', text: 'Enter your current password.' });
    if (newPw.length < 8) return setPasswordMsg({ type: 'error', text: 'New password must be at least 8 characters.' });
    if (!PASSWORD_PATTERN.test(newPw)) {
      return setPasswordMsg({
        type: 'error',
        text: 'Password can only contain letters, numbers, hyphens and underscores. No spaces or special symbols.',
      });
    }
    if (newPw !== confirmPw) return setPasswordMsg({ type: 'error', text: 'New passwords do not match.' });
    if (newPw === currentPw) return setPasswordMsg({ type: 'error', text: 'New password must be different from your current one.' });

    setSavingPassword(true);
    try {
      await changeMemberPassword(token, { current_password: currentPw, new_password: newPw });
      setLoggingOut(true);
      setPasswordMsg({
        type: 'success',
        text: 'Password changed successfully! Logging you out in 3 seconds — log in with your new password.',
      });
      setCurrentPw(''); setNewPw(''); setConfirmPw('');
      setTimeout(() => {
        clearPlayerAuth();
        invalidateStatsCache();
        navigate('/login', { replace: true });
      }, 3000);
    } catch (err) {
      setPasswordMsg({ type: 'error', text: err.response?.data?.detail || 'Could not change password. Try again.' });
    } finally {
      setSavingPassword(false);
    }
  };

  if (loading) {
    return (
      <div className="flex-1 flex items-center justify-center min-h-[60vh]">
        <span className="material-symbols-outlined text-primary text-5xl animate-spin">sports_soccer</span>
      </div>
    );
  }

  const currentBallerName = profile?.baller_name || player?.baller_name || '';
  const currentJersey = profile?.jersey_number ?? player?.jersey_number;

  const pwMatch = confirmPw && newPw && confirmPw === newPw;
  const pwNoMatch = confirmPw && newPw && confirmPw !== newPw;

  return (
    <div className="max-w-2xl mx-auto px-4 py-6 md:py-10 space-y-6">

      {/* ── Header ────────────────────────────────────────────────────────── */}
      <div className="flex items-center gap-4">
        <JerseyAvatar shortName={currentBallerName} number={currentJersey} size="lg" />
        <div>
          <h1 className="text-2xl md:text-3xl font-black leading-tight">{currentBallerName}</h1>
          <p className="text-slate-400 text-sm mt-0.5">
            {profile?.first_name} {profile?.surname}
          </p>
        </div>
      </div>

      {/* ── Account info (read-only) ───────────────────────────────────────── */}
      <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-5">
        <h2 className="font-bold text-primary flex items-center gap-2 mb-4">
          <span className="material-symbols-outlined text-lg">person</span>
          Account info
        </h2>
        <div className="grid grid-cols-2 gap-x-6 gap-y-3 text-sm">
          {[
            { label: 'First name', value: profile?.first_name },
            { label: 'Surname', value: profile?.surname },
            { label: 'Email', value: profile?.email },
            {
              label: 'Member since',
              value: profile?.created_at ? new Date(profile.created_at).getFullYear() : '–',
            },
          ].map(({ label, value }) => (
            <div key={label}>
              <p className="text-slate-500 text-xs uppercase tracking-wide mb-0.5">{label}</p>
              <p className="font-medium truncate">{value || '–'}</p>
            </div>
          ))}
        </div>
        <p className="mt-4 text-xs text-slate-500 flex items-center gap-1.5">
          <span className="material-symbols-outlined text-sm">info</span>
          To change your name or email address, contact the admin.
        </p>
      </div>

      {/* ── Edit details ──────────────────────────────────────────────────── */}
      <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-5">
        <h2 className="font-bold text-primary flex items-center gap-2 mb-4">
          <span className="material-symbols-outlined text-lg">edit</span>
          Edit your details
        </h2>
        <form onSubmit={handleSaveDetails} className="space-y-4">
          <div>
            <label className={labelCls}>Baller name</label>
            <input
              className={inputCls}
              value={ballerName}
              onChange={(e) => setBallerName(e.target.value)}
              maxLength={50}
              placeholder="Your baller name"
              autoComplete="off"
            />
          </div>
          <div>
            <label className={labelCls}>Shirt number</label>
            <input
              className={inputCls}
              type="number"
              min={1}
              max={100}
              value={jerseyNumber}
              onChange={(e) => setJerseyNumber(e.target.value)}
              placeholder="1 – 100"
            />
          </div>
          <div>
            <label className={labelCls}>WhatsApp number</label>
            <input
              className={inputCls}
              value={whatsappPhone}
              onChange={(e) => setWhatsappPhone(e.target.value)}
              maxLength={30}
              placeholder="+234 800 000 0000"
              type="tel"
            />
          </div>
          <StatusBanner msg={detailsMsg} />
          <button
            type="submit"
            disabled={savingDetails || !detailsDirty}
            className="w-full py-2.5 bg-primary text-background-dark font-bold rounded-lg hover:opacity-90 disabled:opacity-50 text-sm transition-opacity"
          >
            {savingDetails ? 'Saving…' : detailsDirty ? 'Save changes' : 'No changes to save'}
          </button>
        </form>
      </div>

      {/* ── Change password ───────────────────────────────────────────────── */}
      <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-5">
        <h2 className="font-bold text-primary flex items-center gap-2 mb-3">
          <span className="material-symbols-outlined text-lg">lock</span>
          Change password
        </h2>

        {/* Warning */}
        <div className="flex items-start gap-2 text-xs text-amber-400 bg-amber-500/10 border border-amber-500/20 rounded-lg px-3 py-2.5 mb-4">
          <span className="material-symbols-outlined text-sm shrink-0 mt-0.5">warning</span>
          <span>Changing your password will <strong>log you out immediately</strong>. You will need to log in again with your new password.</span>
        </div>

        {/* Format hint */}
        <div className="flex items-start gap-2 text-xs text-slate-400 bg-slate-800/60 border border-slate-700/50 rounded-lg px-3 py-2.5 mb-4">
          <span className="material-symbols-outlined text-sm shrink-0 text-primary mt-0.5">info</span>
          <span>
            <strong className="text-slate-300">Password rules:</strong> At least 8 characters.
            Only <strong className="text-slate-200">letters</strong>, <strong className="text-slate-200">numbers</strong>,{' '}
            <strong className="text-slate-200">hyphens</strong> and <strong className="text-slate-200">underscores</strong> are allowed.{' '}
            <span className="text-slate-500">No spaces, @, #, !, etc.</span>
            <br />
            <span className="text-primary font-medium">Good examples:</span>{' '}
            <code className="text-primary">EkoFire24</code>, <code className="text-primary">Baller-99</code>,{' '}
            <code className="text-primary">Goal_King2025</code>
          </span>
        </div>

        <form onSubmit={handleChangePassword} className="space-y-4">
          {/* Current password */}
          <div>
            <label className={labelCls}>Current password</label>
            <div className="relative">
              <input
                className={`${inputCls} pr-10`}
                type={showCurrentPw ? 'text' : 'password'}
                value={currentPw}
                onChange={(e) => setCurrentPw(e.target.value)}
                placeholder="Your current password"
                autoComplete="current-password"
                disabled={loggingOut}
              />
              <ShowHideButton show={showCurrentPw} onToggle={() => setShowCurrentPw((s) => !s)} />
            </div>
          </div>

          {/* New password */}
          <div>
            <label className={labelCls}>New password</label>
            <div className="relative">
              <input
                className={`${inputCls} pr-10`}
                type={showNewPw ? 'text' : 'password'}
                value={newPw}
                onChange={(e) => setNewPw(e.target.value)}
                placeholder="Min 8 characters"
                autoComplete="new-password"
                minLength={8}
                maxLength={50}
                disabled={loggingOut}
              />
              <ShowHideButton show={showNewPw} onToggle={() => setShowNewPw((s) => !s)} />
            </div>
            <PasswordStrengthBar password={newPw} />
          </div>

          {/* Confirm new password */}
          <div>
            <label className={labelCls}>Confirm new password</label>
            <div className="relative">
              <input
                className={`${inputCls} pr-10 ${pwNoMatch ? 'border-red-500/60' : pwMatch ? 'border-green-500/60' : ''}`}
                type={showConfirmPw ? 'text' : 'password'}
                value={confirmPw}
                onChange={(e) => setConfirmPw(e.target.value)}
                placeholder="Re-enter new password"
                autoComplete="new-password"
                maxLength={50}
                disabled={loggingOut}
              />
              <ShowHideButton show={showConfirmPw} onToggle={() => setShowConfirmPw((s) => !s)} />
            </div>
            {pwMatch && <p className="text-xs text-green-400 mt-1 flex items-center gap-1"><span className="material-symbols-outlined text-sm">check_circle</span>Passwords match</p>}
            {pwNoMatch && <p className="text-xs text-red-400 mt-1 flex items-center gap-1"><span className="material-symbols-outlined text-sm">cancel</span>Passwords do not match</p>}
          </div>

          <StatusBanner msg={passwordMsg} />

          <button
            type="submit"
            disabled={savingPassword || loggingOut}
            className="w-full py-2.5 bg-red-600/80 hover:bg-red-600 text-white font-bold rounded-lg disabled:opacity-60 text-sm transition-colors"
          >
            {loggingOut ? 'Logging you out…' : savingPassword ? 'Changing password…' : 'Change password & log out'}
          </button>
        </form>
      </div>

    </div>
  );
}
