import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  adminLogin,
  getPending,
  approvePlayer,
  rejectPlayer,
  getApproved,
  suspendPlayer,
  activatePlayer,
  deletePlayer,
  setDues,
  getDuesByQuarter,
  getPaymentEvidence,
  getPaymentEvidenceFile,
  approvePayment,
  rejectPayment,
  listAdminMatchdays,
  createMatchday,
  getAdminMatchday,
  voteAllMatchday,
  closeVotingMatchday,
  reopenVotingMatchday,
  voteAddMatchday,
  voteRemoveMatchday,
  approveMatchday,
  rejectMatchday,
  deleteMatchday,
  getAdminMatchdayGroups,
  getMatchdayAttendance,
  getMatchdayAttendanceSummary,
  setMatchdayAttendance,
  setMatchdayAttendanceBulk,
  getMatchdayCards,
  addMatchdayCard,
  getFixtureCards,
  moveMatchdayMember,
  moveMatchdayMemberBatch,
  unpublishMatchdayGroups,
  regenerateMatchdayGroups,
  publishMatchdayGroups,
  generateFixtures,
  getFixtures,
  publishFixtures,
  startFixture,
  addGoal,
  removeGoal,
  getFixtureGoals,
  endFixture,
  endMatchday,
  reopenMatchday,
  getMatchdayTable,
  getMatchdayPlayerRatings,
} from '../api';
import './Admin.css';

const ADMIN_TOKEN_KEY = 'eko_football_admin_token';

function getAdminToken() {
  return localStorage.getItem(ADMIN_TOKEN_KEY);
}

function setAdminToken(token) {
  if (token) localStorage.setItem(ADMIN_TOKEN_KEY, token);
  else localStorage.removeItem(ADMIN_TOKEN_KEY);
}

const QUARTERS = [
  { q: 1, label: 'Jan–Mar' },
  { q: 2, label: 'Apr–Jun' },
  { q: 3, label: 'Jul–Sep' },
  { q: 4, label: 'Oct–Dec' },
];

export default function Admin() {
  const navigate = useNavigate();
  const [loggedIn, setLoggedIn] = useState(!!getAdminToken());
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(true);
  const [loginError, setLoginError] = useState('');
  const [loading, setLoading] = useState(false);
  const [tab, setTab] = useState('pending'); // 'pending' | 'approved' | 'evidence' | 'dues' | 'matchday'
  const [pending, setPending] = useState([]);
  const [approved, setApproved] = useState([]);
  const [currentYear, setCurrentYear] = useState(new Date().getFullYear());
  const [currentQuarter, setCurrentQuarter] = useState(Math.ceil((new Date().getMonth() + 1) / 3));
  const [duesYear, setDuesYear] = useState(new Date().getFullYear());
  const [duesQuarter, setDuesQuarter] = useState(Math.ceil((new Date().getMonth() + 1) / 3));
  const [duesMembers, setDuesMembers] = useState([]);
  const [loadingDues, setLoadingDues] = useState(false);
  const [paymentEvidence, setPaymentEvidence] = useState([]);
  const [matchdaysList, setMatchdaysList] = useState([]);
  const [selectedMatchdayId, setSelectedMatchdayId] = useState(null);
  const [matchdayData, setMatchdayData] = useState(null);
  const [matchdayGroups, setMatchdayGroups] = useState(null);
  const [matchdayAttendance, setMatchdayAttendance] = useState([]);
  const [matchdayCards, setMatchdayCards] = useState([]);
  const [matchdayFixtures, setMatchdayFixtures] = useState([]);
  const [matchdayTable, setMatchdayTable] = useState([]);
  const [matchdayPlayerRatings, setMatchdayPlayerRatings] = useState([]);
  const [newMatchdayDate, setNewMatchdayDate] = useState('');
  const [loadingList, setLoadingList] = useState(false);
  const [actionMsg, setActionMsg] = useState('');
  const [moveFrom, setMoveFrom] = useState(null);
  const [moveTo, setMoveTo] = useState(null);
  const [movePlayerId, setMovePlayerId] = useState(null);
  const [votingAll, setVotingAll] = useState(false);
  const [addGoalFixtureId, setAddGoalFixtureId] = useState(null);
  const [addGoalScorer, setAddGoalScorer] = useState('');
  const [addGoalAssister, setAddGoalAssister] = useState('');
  const [pendingMoves, setPendingMoves] = useState([]);
  const [attendanceSummary, setAttendanceSummary] = useState({ present: [], absent: [] });
  const [groupsSectionOpen, setGroupsSectionOpen] = useState(false);
  const [addCardFixtureId, setAddCardFixtureId] = useState(null);
  const [addCardType, setAddCardType] = useState(null);
  const [addCardPlayerId, setAddCardPlayerId] = useState('');
  const [menuOpen, setMenuOpen] = useState(false);
  const [toastDismissAt, setToastDismissAt] = useState(null);

  const fetchPending = async () => {
    const token = getAdminToken();
    if (!token) return;
    setLoadingList(true);
    try {
      const data = await getPending(token);
      setPending(data.pending || []);
    } catch {
      setPending([]);
    } finally {
      setLoadingList(false);
    }
  };

  const fetchApproved = async () => {
    const token = getAdminToken();
    if (!token) return;
    setLoadingList(true);
    try {
      const data = await getApproved(token);
      setApproved(data.approved || []);
      setCurrentYear(data.current_year ?? new Date().getFullYear());
      setCurrentQuarter(data.current_quarter ?? Math.ceil((new Date().getMonth() + 1) / 3));
    } catch {
      setApproved([]);
    } finally {
      setLoadingList(false);
    }
  };

  const fetchDuesByQuarter = async () => {
    const token = getAdminToken();
    if (!token) return;
    setLoadingDues(true);
    try {
      const data = await getDuesByQuarter(token, duesYear, duesQuarter);
      setDuesMembers(data.members || []);
    } catch {
      setDuesMembers([]);
    } finally {
      setLoadingDues(false);
    }
  };

  const fetchPaymentEvidence = async () => {
    const token = getAdminToken();
    if (!token) return;
    setLoadingList(true);
    try {
      const data = await getPaymentEvidence(token);
      setPaymentEvidence(data.pending || []);
    } catch {
      setPaymentEvidence([]);
    } finally {
      setLoadingList(false);
    }
  };

  const fetchMatchdays = async () => {
    const token = getAdminToken();
    if (!token) return;
    setLoadingList(true);
    try {
      const data = await listAdminMatchdays(token);
      setMatchdaysList(data.matchdays || []);
      if (selectedMatchdayId) {
        // Run all independent requests in parallel — cuts wait time from ~9 sequential to ~1 round-trip
        const [detailRes, groupsRes, attRes, attSumRes, cardsRes, fixRes, tableRes, ratingsRes] = await Promise.allSettled([
          getAdminMatchday(selectedMatchdayId, token),
          getAdminMatchdayGroups(selectedMatchdayId, token),
          getMatchdayAttendance(selectedMatchdayId, token),
          getMatchdayAttendanceSummary(selectedMatchdayId, token),
          getMatchdayCards(selectedMatchdayId, token),
          getFixtures(selectedMatchdayId, token),
          getMatchdayTable(selectedMatchdayId, token),
          getMatchdayPlayerRatings(selectedMatchdayId, token),
        ]);
        const detail = detailRes.status === 'fulfilled' ? detailRes.value : null;
        setMatchdayData(detail);
        setMatchdayGroups(groupsRes.status === 'fulfilled' ? groupsRes.value : null);
        setMatchdayAttendance(attRes.status === 'fulfilled' ? attRes.value.attendance || [] : []);
        if (detail?.matchday?.groups_published && attSumRes.status === 'fulfilled') {
          setAttendanceSummary({ present: attSumRes.value.present || [], absent: attSumRes.value.absent || [] });
        } else {
          setAttendanceSummary({ present: [], absent: [] });
        }
        setMatchdayCards(cardsRes.status === 'fulfilled' ? cardsRes.value.cards || [] : []);
        setMatchdayFixtures(fixRes.status === 'fulfilled' ? fixRes.value.fixtures || [] : []);
        setMatchdayTable(tableRes.status === 'fulfilled' ? tableRes.value.table || [] : []);
        setMatchdayPlayerRatings(ratingsRes.status === 'fulfilled' ? ratingsRes.value.ratings || [] : []);
      } else {
        setMatchdayData(null);
        setMatchdayGroups(null);
        setMatchdayAttendance([]);
        setMatchdayCards([]);
        setMatchdayFixtures([]);
        setMatchdayTable([]);
        setMatchdayPlayerRatings([]);
      }
    } catch {
      setMatchdaysList([]);
      setMatchdayData(null);
      setMatchdayGroups(null);
    } finally {
      setLoadingList(false);
    }
  };

  // Lightweight refresh after goal/card actions — only re-fetches what changed (fixtures, table, ratings, cards)
  // instead of the full 9-request fetchMatchdays, cutting post-action wait from ~60s to ~3s.
  const refreshFixtures = async (matchdayId) => {
    const token = getAdminToken();
    if (!token || !matchdayId) return;
    const [fixRes, tableRes, ratingsRes, cardsRes] = await Promise.allSettled([
      getFixtures(matchdayId, token),
      getMatchdayTable(matchdayId, token),
      getMatchdayPlayerRatings(matchdayId, token),
      getMatchdayCards(matchdayId, token),
    ]);
    if (fixRes.status === 'fulfilled') setMatchdayFixtures(fixRes.value.fixtures || []);
    if (tableRes.status === 'fulfilled') setMatchdayTable(tableRes.value.table || []);
    if (ratingsRes.status === 'fulfilled') setMatchdayPlayerRatings(ratingsRes.value.ratings || []);
    if (cardsRes.status === 'fulfilled') setMatchdayCards(cardsRes.value.cards || []);
  };

  useEffect(() => {
    if (loggedIn) {
      if (tab === 'pending') fetchPending();
      else if (tab === 'approved') fetchApproved();
      else if (tab === 'evidence') fetchPaymentEvidence();
      else if (tab === 'matchday') fetchMatchdays();
      else if (tab === 'dues') fetchDuesByQuarter();
    }
  }, [loggedIn, tab, selectedMatchdayId]);
  useEffect(() => {
    if (loggedIn && tab === 'dues') fetchDuesByQuarter();
  }, [duesYear, duesQuarter]);

  const handleAdminLogin = async (e) => {
    e.preventDefault();
    setLoginError('');
    setLoading(true);
    try {
      const data = await adminLogin(username, password);
      setAdminToken(data.token);
      setLoggedIn(true);
    } catch (err) {
      setLoginError(err.response?.data?.detail || 'Login failed.');
    } finally {
      setLoading(false);
    }
  };

  const showToast = (msg) => {
    if (msg === '' || msg == null) {
      setActionMsg('');
      setToastDismissAt(null);
      return;
    }
    setActionMsg(msg);
    setToastDismissAt(Date.now() + 8000);
  };
  const dismissToast = () => {
    setActionMsg('');
    setToastDismissAt(null);
  };
  useEffect(() => {
    if (!actionMsg || !toastDismissAt) return;
    const t = setTimeout(dismissToast, Math.max(0, toastDismissAt - Date.now()));
    return () => clearTimeout(t);
  }, [actionMsg, toastDismissAt]);

  const handleApprove = async (id) => {
    const token = getAdminToken();
    if (!token) return;
    showToast('');
    try {
      const data = await approvePlayer(id, token);
      showToast(data?.message ?? 'Approved and email sent.');
      if (data?.success !== false) fetchPending();
    } catch (err) {
      showToast(err.response?.data?.detail ?? err.response?.data?.message ?? 'Approve failed.');
    }
  };

  const handleReject = async (id) => {
    const token = getAdminToken();
    if (!token) return;
    showToast('');
    try {
      await rejectPlayer(id, token);
      showToast('Rejected.');
      fetchPending();
    } catch (err) {
      showToast(err.response?.data?.detail || 'Reject failed.');
    }
  };

  const handleSuspend = async (playerId) => {
    const token = getAdminToken();
    if (!token) return;
    showToast('');
    try {
      await suspendPlayer(playerId, token);
      showToast('Member suspended.');
      fetchApproved();
    } catch (err) {
      showToast(err.response?.data?.detail || 'Suspend failed.');
    }
  };

  const handleActivate = async (playerId) => {
    const token = getAdminToken();
    if (!token) return;
    showToast('');
    try {
      await activatePlayer(playerId, token);
      showToast('Member activated.');
      fetchApproved();
    } catch (err) {
      showToast(err.response?.data?.detail || 'Activate failed.');
    }
  };

  const handleSetDues = async (playerId, year, quarter, status, waiverDueBy = null) => {
    const token = getAdminToken();
    if (!token) return;
    showToast('');
    if (status === 'waiver' && !waiverDueBy) {
      const d = new Date();
      d.setDate(d.getDate() + 14);
      waiverDueBy = d.toISOString().slice(0, 10);
    }
    try {
      await setDues(playerId, year, quarter, status, token, waiverDueBy);
      showToast(`Dues set to ${status}.`);
      fetchApproved();
      if (tab === 'dues') fetchDuesByQuarter();
    } catch (err) {
      showToast(err.response?.data?.detail || 'Set dues failed.');
    }
  };

  const handleApprovePayment = async (evidenceId) => {
    const token = getAdminToken();
    if (!token) return;
    showToast('');
    try {
      await approvePayment(evidenceId, token);
      showToast('Payment approved. Attachment will be emailed; file deleted after 1 week.');
      fetchPaymentEvidence();
    } catch (err) {
      showToast(err.response?.data?.detail || 'Approve failed.');
    }
  };

  const handleRejectPayment = async (evidenceId) => {
    const token = getAdminToken();
    if (!token) return;
    showToast('');
    try {
      await rejectPayment(evidenceId, token);
      showToast('Payment evidence rejected.');
      fetchPaymentEvidence();
    } catch (err) {
      showToast(err.response?.data?.detail || 'Reject failed.');
    }
  };

  const handleCreateMatchday = async (e) => {
    e.preventDefault();
    if (!newMatchdayDate) return;
    const token = getAdminToken();
    if (!token) return;
    showToast('');
    try {
      const data = await createMatchday(newMatchdayDate, token);
      showToast(data.message || 'Matchday created.');
      setNewMatchdayDate('');
      fetchMatchdays();
      if (data.matchday?.id) setSelectedMatchdayId(data.matchday.id);
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed.');
    }
  };

  const handleVoteAll = async () => {
    if (!selectedMatchdayId) return;
    const token = getAdminToken();
    if (!token) return;
    showToast('');
    setVotingAll(true);
    try {
      const data = await voteAllMatchday(selectedMatchdayId, token);
      showToast(data.message || 'Votes recorded.');
      fetchMatchdays();
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed.');
    } finally {
      setVotingAll(false);
    }
  };

  const handleApproveMatchday = async () => {
    if (!selectedMatchdayId) return;
    const token = getAdminToken();
    if (!token) return;
    showToast('');
    try {
      await approveMatchday(selectedMatchdayId, token);
      showToast('Matchday approved. Assign groups and publish when ready.');
      fetchMatchdays();
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed.');
    }
  };

  const handleRejectMatchday = async () => {
    if (!selectedMatchdayId) return;
    const token = getAdminToken();
    if (!token) return;
    showToast('');
    try {
      await rejectMatchday(selectedMatchdayId, token);
      showToast('Matchday rejected.');
      fetchMatchdays();
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed.');
    }
  };

  const handleDeleteMatchday = async () => {
    if (!selectedMatchdayId) return;
    if (!window.confirm('Permanently delete this matchday and all its votes, groups, fixtures and goals? This cannot be undone.')) return;
    const token = getAdminToken();
    if (!token) return;
    showToast('');
    try {
      await deleteMatchday(selectedMatchdayId, token);
      showToast('Matchday deleted.');
      setSelectedMatchdayId(null);
      setMatchdayData(null);
      fetchMatchdays();
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed.');
    }
  };

  const handleDeletePlayer = async (playerId, ballerName) => {
    if (!window.confirm(`Permanently delete member "${ballerName}"? All their data (dues, votes, goals, etc.) will be removed. This cannot be undone.`)) return;
    const token = getAdminToken();
    if (!token) return;
    showToast('');
    try {
      await deletePlayer(playerId, token);
      showToast('Member deleted.');
      fetchApproved();
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed.');
    }
  };

  const handleMoveMember = async () => {
    if (moveFrom == null || moveTo == null || movePlayerId == null || !selectedMatchdayId) return;
    const token = getAdminToken();
    if (!token) return;
    showToast('');
    try {
      await moveMatchdayMember(selectedMatchdayId, { from_group_id: moveFrom, to_group_id: moveTo, player_id: movePlayerId }, token);
      showToast('Member moved.');
      setMoveFrom(null); setMoveTo(null); setMovePlayerId(null);
      fetchMatchdays();
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed.');
    }
  };

  const addPendingMove = (fromGroupId, toGroupId, playerId) => {
    if (!toGroupId) return;
    setPendingMoves((prev) => {
      const without = prev.filter((m) => m.player_id !== playerId);
      return [...without, { from_group_id: fromGroupId, to_group_id: toGroupId, player_id: playerId }];
    });
  };

  const removePendingMove = (playerId) => {
    setPendingMoves((prev) => prev.filter((m) => m.player_id !== playerId));
  };

  const handleApplyBatchMoves = async () => {
    if (pendingMoves.length === 0 || !selectedMatchdayId) return;
    const token = getAdminToken();
    if (!token) return;
    showToast('');
    try {
      await moveMatchdayMemberBatch(selectedMatchdayId, pendingMoves, token);
      showToast(`Moved ${pendingMoves.length} member(s).`);
      setPendingMoves([]);
      setMoveFrom(null); setMoveTo(null); setMovePlayerId(null);
      fetchMatchdays();
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed.');
    }
  };

  const handleUnpublishGroups = async () => {
    if (!selectedMatchdayId) return;
    const token = getAdminToken();
    if (!token) return;
    showToast('');
    try {
      await unpublishMatchdayGroups(selectedMatchdayId, token);
      showToast('Groups unpublished. You can edit and re-publish.');
      setGroupsSectionOpen(true);
      fetchMatchdays();
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed.');
    }
  };

  const handlePublishGroups = async () => {
    if (!selectedMatchdayId) return;
    const token = getAdminToken();
    if (!token) return;
    showToast('');
    try {
      await publishMatchdayGroups(selectedMatchdayId, token);
      showToast('Groups published. Members can see their group.');
      fetchMatchdays();
    } catch (err) {
      showToast(err.response?.data?.detail || 'Failed.');
    }
  };

  const handleViewEvidence = async (evidenceId, fileName) => {
    const token = getAdminToken();
    if (!token) return;
    try {
      const blob = await getPaymentEvidenceFile(evidenceId, token);
      const url = URL.createObjectURL(blob);
      const w = window.open(url, '_blank', 'noopener');
      if (w) w.focus();
      setTimeout(() => URL.revokeObjectURL(url), 60000);
    } catch (err) {
      let message = 'Could not open file.';
      if (err.response?.data instanceof Blob) {
        try {
          const text = await err.response.data.text();
          const obj = JSON.parse(text);
          if (obj.detail) message = obj.detail;
        } catch (_) {}
      } else if (err.response?.data?.detail) {
        message = err.response.data.detail;
      }
      showToast(message);
    }
  };

  const handleLogout = () => {
    setAdminToken(null);
    setLoggedIn(false);
    setPending([]);
  };

  if (!loggedIn) {
    return (
      <div className="min-h-screen bg-background-dark flex items-center justify-center p-4 font-display">
        <div className="w-full max-w-md bg-slate-900/60 border border-primary/10 rounded-xl p-8 shadow-xl">
          <div className="flex items-center gap-3 mb-6">
            <div className="bg-primary size-12 rounded flex items-center justify-center text-background-dark">
              <span className="material-symbols-outlined text-3xl font-bold">admin_panel_settings</span>
            </div>
            <div>
              <h1 className="text-xl font-bold text-slate-100">Admin</h1>
              <p className="text-xs text-primary font-medium uppercase tracking-wider">Eko Football</p>
            </div>
          </div>
          <p className="text-slate-400 text-sm mb-6">Log in to manage sign-ups, dues and matchdays.</p>
          <form onSubmit={handleAdminLogin} className="flex flex-col gap-4">
            <label className="text-sm font-medium text-slate-300">Username</label>
            <input
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              className="w-full px-4 py-3 rounded-lg bg-slate-800 border border-slate-600 text-slate-100 placeholder-slate-500 focus:border-primary focus:outline-none"
              required
            />
            <label className="text-sm font-medium text-slate-300">Password</label>
            <div className="flex gap-2 items-center">
              <input
                type={showPassword ? 'text' : 'password'}
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="flex-1 px-4 py-3 rounded-lg bg-slate-800 border border-slate-600 text-slate-100 placeholder-slate-500 focus:border-primary focus:outline-none"
                required
              />
              <label className="flex items-center gap-2 text-slate-400 text-sm cursor-pointer whitespace-nowrap">
                <input type="checkbox" checked={showPassword} onChange={(e) => setShowPassword(e.target.checked)} className="rounded" />
                Show
              </label>
            </div>
            {loginError && <div className="text-red-400 text-sm">{loginError}</div>}
            <button type="submit" className="mt-2 py-3 px-4 bg-primary text-background-dark font-bold rounded-lg hover:shadow-[0_0_20px_rgba(10,194,71,0.3)] transition-all disabled:opacity-60" disabled={loading}>
              {loading ? 'Logging in...' : 'Log in'}
            </button>
          </form>
          <p className="mt-4 text-slate-500 text-xs">Default: username <code className="bg-slate-800 px-1 rounded">admin</code>, password <code className="bg-slate-800 px-1 rounded">admin123</code>. Override with ADMIN_USERNAME / ADMIN_PASSWORD in the server .env.</p>
        </div>
      </div>
    );
  }

  const navClass = "flex items-center gap-3 px-3 py-3 min-h-[44px] rounded-lg text-slate-400 hover:bg-primary/5 hover:text-primary transition-colors touch-manipulation w-full text-left";
  const navActive = "bg-primary/10 text-primary font-semibold";
  const closeMenu = () => setMenuOpen(false);

  return (
    <div className="bg-background-dark text-slate-100 min-h-screen min-h-[100dvh] flex overflow-hidden font-display">
      {/* Fixed toast: visible without scrolling */}
      {actionMsg && (
        <div className="fixed top-0 left-0 right-0 z-[100] flex justify-center p-3 safe-area-inset-top">
          <div className="bg-primary/95 text-background-dark px-4 py-3 rounded-xl shadow-lg flex items-center gap-3 max-w-md w-full border border-primary">
            <span className="flex-1 text-sm font-medium">{actionMsg}</span>
            <button type="button" onClick={dismissToast} className="shrink-0 min-w-[36px] min-h-[36px] flex items-center justify-center rounded-lg hover:bg-black/20" aria-label="Dismiss">
              <span className="material-symbols-outlined text-xl">close</span>
            </button>
          </div>
        </div>
      )}
      {/* Mobile menu backdrop */}
      <div
        className="fixed inset-0 bg-black/50 z-40 md:hidden transition-opacity"
        style={{ opacity: menuOpen ? 1 : 0, pointerEvents: menuOpen ? 'auto' : 'none' }}
        onClick={closeMenu}
        aria-hidden="true"
      />
      {/* Sidebar: drawer on mobile, inline on desktop */}
      <aside
        className={`
          w-64 border-r border-primary/10 bg-background-dark flex flex-col h-screen z-50
          fixed inset-y-0 left-0 transform transition-transform duration-200 ease-out
          md:relative md:translate-x-0 md:shrink-0
          ${menuOpen ? 'translate-x-0' : '-translate-x-full'}
        `}
      >
        <div className="p-4 md:p-6 flex items-center gap-3">
          <div className="bg-primary size-10 rounded flex items-center justify-center text-background-dark shrink-0">
            <span className="material-symbols-outlined font-bold text-2xl">admin_panel_settings</span>
          </div>
          <div className="min-w-0">
            <h1 className="text-lg font-bold leading-tight">Eko Football</h1>
            <p className="text-xs text-primary font-medium uppercase tracking-wider">Admin Portal</p>
          </div>
        </div>
        <nav className="flex-1 px-4 space-y-1 mt-4">
          <button type="button" className={`${navClass} ${tab === 'pending' ? navActive : ''}`} onClick={() => { setTab('pending'); closeMenu(); }}>
            <span className="material-symbols-outlined shrink-0">person_add</span>
            <span className="text-sm">Pending sign-ups</span>
          </button>
          <button type="button" className={`${navClass} ${tab === 'approved' ? navActive : ''}`} onClick={() => { setTab('approved'); closeMenu(); }}>
            <span className="material-symbols-outlined shrink-0">groups</span>
            <span className="text-sm">Approved members</span>
          </button>
          <button type="button" className={`${navClass} ${tab === 'evidence' ? navActive : ''}`} onClick={() => { setTab('evidence'); closeMenu(); }}>
            <span className="material-symbols-outlined shrink-0">receipt_long</span>
            <span className="text-sm">Payment evidence</span>
          </button>
          <button type="button" className={`${navClass} ${tab === 'dues' ? navActive : ''}`} onClick={() => { setTab('dues'); closeMenu(); }}>
            <span className="material-symbols-outlined shrink-0">payments</span>
            <span className="text-sm">Quarterly dues</span>
          </button>
          <button type="button" className={`${navClass} ${tab === 'matchday' ? navActive : ''}`} onClick={() => { setTab('matchday'); closeMenu(); }}>
            <span className="material-symbols-outlined shrink-0">calendar_month</span>
            <span className="text-sm">Matchday</span>
          </button>
        </nav>
        <div className="p-4 border-t border-primary/10">
          <button type="button" onClick={handleLogout} className="flex items-center gap-3 w-full px-3 py-3 min-h-[44px] rounded-lg text-slate-400 hover:bg-primary/5 hover:text-primary transition-colors touch-manipulation">
            <span className="material-symbols-outlined">logout</span>
            <span className="text-sm">Log out</span>
          </button>
        </div>
      </aside>

      {/* Main: mobile header + content */}
      <div className="w-full md:min-w-0 flex-1 flex flex-col min-w-0">
        <header className="min-h-14 md:h-20 border-b border-primary/10 px-4 md:px-8 flex items-center gap-3 sticky top-0 bg-background-dark/80 backdrop-blur-md z-30 safe-area-inset-top">
          <button
            type="button"
            onClick={() => setMenuOpen(true)}
            className="md:hidden min-w-[44px] min-h-[44px] flex items-center justify-center text-slate-400 hover:text-primary touch-manipulation rounded-lg -ml-2"
            aria-label="Open menu"
          >
            <span className="material-symbols-outlined text-2xl">menu</span>
          </button>
          <h2 className="text-lg md:text-xl font-bold truncate flex-1">
            {tab === 'pending' && 'Pending sign-ups'}
            {tab === 'approved' && 'Approved members'}
            {tab === 'evidence' && 'Payment evidence'}
            {tab === 'dues' && 'Quarterly dues'}
            {tab === 'matchday' && 'Matchday'}
          </h2>
        </header>

      <main className="flex-1 overflow-y-auto overflow-x-hidden custom-scrollbar flex flex-col min-w-0">
        <div className="p-4 md:p-8 max-w-6xl mx-auto w-full space-y-6 overflow-x-hidden">
        {tab === 'pending' && (
          <>
            <div className="flex justify-end mb-2">
              <button
                type="button"
                onClick={() => {
                  const link = `${window.location.origin}/signup`;
                  navigator.clipboard.writeText(link).then(() => showToast('Signup link copied!'));
                }}
                className="flex items-center gap-2 py-2 px-4 bg-slate-700 text-slate-200 font-semibold rounded-lg hover:bg-slate-600 text-sm transition-colors"
              >
                <span className="material-symbols-outlined text-base">link</span>
                Copy signup link
              </button>
            </div>
            {loadingList ? (
              <p className="text-slate-400">Loading...</p>
            ) : pending.length === 0 ? (
              <p className="text-slate-500 text-center py-12">No pending sign-ups.</p>
            ) : (
              <ul className="space-y-4">
                {pending.map((p) => (
                  <li key={p.id} className="bg-slate-900/40 border border-primary/10 rounded-xl p-6 flex flex-col sm:flex-row sm:items-center justify-between gap-4">
                    <div className="text-slate-200 text-sm">
                      <strong className="text-slate-100 text-base">{p.baller_name}</strong> – {p.first_name} {p.surname}
                      <br />
                      #{p.jersey_number} · {p.email} · {p.whatsapp_phone}
                      <br />
                      <span className="text-slate-500">{p.created_at}</span>
                    </div>
                    <div className="flex gap-2 shrink-0">
                      <button type="button" className="py-2 px-4 bg-primary text-background-dark font-bold rounded-lg hover:shadow-[0_0_16px_rgba(10,194,71,0.3)] transition-all" onClick={() => handleApprove(p.id)}>Approve</button>
                      <button type="button" className="py-2 px-4 bg-slate-700 text-slate-200 font-semibold rounded-lg hover:bg-slate-600 transition-colors" onClick={() => handleReject(p.id)}>Reject</button>
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </>
        )}

        {tab === 'approved' && (
          <>
            <p className="text-slate-400 text-sm mb-4">Dues period: {currentYear} Q{currentQuarter} ({QUARTERS.find(x => x.q === currentQuarter)?.label ?? ''})</p>
            {loadingList ? (
              <p className="text-slate-400">Loading...</p>
            ) : approved.length === 0 ? (
              <p className="text-slate-500 text-center py-12">No approved members.</p>
            ) : (
              <div className="bg-slate-900/40 border border-primary/10 rounded-xl overflow-hidden">
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-slate-700 bg-slate-800/50">
                        <th className="text-left py-4 px-4 font-semibold text-slate-300">Baller name</th>
                        <th className="text-left py-4 px-4 font-semibold text-slate-300">Password</th>
                        <th className="text-left py-4 px-4 font-semibold text-slate-300">Dues</th>
                        <th className="text-left py-4 px-4 font-semibold text-slate-300">Status</th>
                        <th className="text-left py-4 px-4 font-semibold text-slate-300">Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {approved.map((m) => (
                        <tr key={m.id} className="border-b border-slate-700/50 hover:bg-primary/5">
                          <td className="py-3 px-4"><strong className="text-slate-100">{m.baller_name}</strong><br /><span className="text-slate-500">{m.first_name} {m.surname} · #{m.jersey_number}</span></td>
                          <td className="py-3 px-4"><code className="bg-slate-800 px-2 py-1 rounded text-xs">{m.password_display || '—'}</code></td>
                          <td className="py-3 px-4">{m.dues_status === 'paid' ? <span className="text-primary font-medium">Paid</span> : m.dues_status === 'waiver' ? (m.waiver_due_by ? `Waiver (by ${m.waiver_due_by})` : 'Waiver') : <span className="text-amber-400">Owing</span>}</td>
                          <td className="py-3 px-4">{m.suspended ? <span className="px-2 py-0.5 rounded text-xs bg-red-500/20 text-red-400">Suspended</span> : <span className="px-2 py-0.5 rounded text-xs bg-primary/20 text-primary">Active</span>}</td>
                          <td className="py-3 px-4 flex flex-wrap gap-2 items-center">
                            <select
                              value={m.dues_status}
                              onChange={(e) => handleSetDues(m.id, m.dues_year, m.dues_quarter, e.target.value)}
                              className="rounded-lg bg-slate-800 border border-slate-600 px-2 py-1.5 text-sm text-slate-100"
                            >
                              <option value="owing">Owing</option>
                              <option value="paid">Paid</option>
                              <option value="waiver">Waiver</option>
                            </select>
                            {m.suspended ? (
                              <button type="button" className="py-1.5 px-3 bg-primary text-background-dark font-semibold rounded-lg text-sm hover:opacity-90" onClick={() => handleActivate(m.id)}>Activate</button>
                            ) : (
                              <button type="button" className="py-1.5 px-3 bg-slate-700 text-slate-200 font-semibold rounded-lg text-sm hover:bg-slate-600" onClick={() => handleSuspend(m.id)}>Suspend</button>
                            )}
                            <button type="button" className="py-1.5 px-3 bg-red-500/20 text-red-400 font-semibold rounded-lg text-sm hover:bg-red-500/30 border border-red-500/40" onClick={() => handleDeletePlayer(m.id, m.baller_name)}>Delete</button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </>
        )}

        {tab === 'dues' && (
          <>
            <div className="flex flex-wrap items-center gap-4 mb-6">
              <label className="flex items-center gap-2 text-sm text-slate-300">
                <span>Year</span>
                <select
                  value={duesYear}
                  onChange={(e) => setDuesYear(Number(e.target.value))}
                  className="rounded-lg bg-slate-800 border border-slate-600 px-3 py-2 text-slate-100"
                >
                  {[new Date().getFullYear(), new Date().getFullYear() - 1, new Date().getFullYear() - 2, new Date().getFullYear() - 3].map((y) => (
                    <option key={y} value={y}>{y}</option>
                  ))}
                </select>
              </label>
              <label className="flex items-center gap-2 text-sm text-slate-300">
                <span>Quarter</span>
                <select
                  value={duesQuarter}
                  onChange={(e) => setDuesQuarter(Number(e.target.value))}
                  className="rounded-lg bg-slate-800 border border-slate-600 px-3 py-2 text-slate-100"
                >
                  {QUARTERS.map(({ q, label }) => (
                    <option key={q} value={q}>{q} – {label}</option>
                  ))}
                </select>
              </label>
              <p className="text-slate-400 text-sm">{duesYear} Q{duesQuarter} ({QUARTERS.find((x) => x.q === duesQuarter)?.label ?? ''})</p>
            </div>
            {loadingDues ? (
              <p className="text-slate-400">Loading...</p>
            ) : duesMembers.length === 0 ? (
              <p className="text-slate-500 text-center py-12">No approved members.</p>
            ) : (
              <div className="bg-slate-900/40 border border-primary/10 rounded-xl overflow-hidden">
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-slate-700 bg-slate-800/50">
                        <th className="text-left py-4 px-4 font-semibold text-slate-300">Member</th>
                        <th className="text-left py-4 px-4 font-semibold text-slate-300">Status</th>
                        <th className="text-left py-4 px-4 font-semibold text-slate-300">Set status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {duesMembers.map((m) => (
                        <tr key={m.id} className="border-b border-slate-700/50 hover:bg-primary/5">
                          <td className="py-3 px-4">
                            <strong className="text-slate-100">{m.baller_name}</strong>
                            <br />
                            <span className="text-slate-500">{m.first_name} {m.surname} · #{m.jersey_number}</span>
                          </td>
                          <td className="py-3 px-4">
                            {m.display_status === 'waiver_overdue' ? (
                              <span className="text-amber-400 font-medium">Waiver (didn&apos;t pay)</span>
                            ) : m.dues_status === 'paid' ? (
                              <span className="text-primary font-medium">Paid</span>
                            ) : m.dues_status === 'waiver' ? (
                              <span className="text-slate-300">Waiver{m.waiver_due_by ? ` (by ${m.waiver_due_by})` : ''}</span>
                            ) : (
                              <span className="text-amber-400 font-medium">Owing</span>
                            )}
                          </td>
                          <td className="py-3 px-4">
                            <select
                              value={m.raw_status}
                              onChange={(e) => handleSetDues(m.id, m.dues_year, m.dues_quarter, e.target.value)}
                              className="rounded-lg bg-slate-800 border border-slate-600 px-2 py-1.5 text-sm text-slate-100"
                            >
                              <option value="owing">Owing</option>
                              <option value="paid">Paid</option>
                              <option value="waiver">Waiver</option>
                            </select>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="px-4 py-3 border-t border-slate-700 bg-slate-800/30 text-xs text-slate-400">
                  Paid: {duesMembers.filter((m) => m.dues_status === 'paid').length} · Owing: {duesMembers.filter((m) => m.dues_status === 'owing' || m.display_status === 'waiver_overdue').length} · Waiver: {duesMembers.filter((m) => m.dues_status === 'waiver').length}
                </div>
              </div>
            )}
          </>
        )}

        {tab === 'evidence' && (
          <>
            {loadingList ? (
              <p className="text-slate-400">Loading...</p>
            ) : paymentEvidence.length === 0 ? (
              <p className="text-slate-500 text-center py-12">No payment evidence pending.</p>
            ) : (
              <ul className="space-y-4">
                {paymentEvidence.map((e) => (
                  <li key={e.id} className="bg-slate-900/40 border border-primary/10 rounded-xl p-6 flex flex-col sm:flex-row sm:items-center justify-between gap-4">
                    <div className="text-slate-200 text-sm">
                      <strong className="text-slate-100">{e.baller_name}</strong> ({e.first_name} {e.surname}) – {e.year} Q{e.quarter}
                      <br />
                      <span className="text-slate-500">File: {e.file_name} · {e.submitted_at}</span>
                    </div>
                    <div className="flex gap-2 shrink-0">
                      <button type="button" className="py-2 px-4 bg-slate-600 text-slate-100 font-semibold rounded-lg hover:bg-slate-500 transition-colors" onClick={() => handleViewEvidence(e.id, e.file_name)}>View evidence</button>
                      <button type="button" className="py-2 px-4 bg-primary text-background-dark font-bold rounded-lg hover:shadow-[0_0_16px_rgba(10,194,71,0.3)] transition-all" onClick={() => handleApprovePayment(e.id)}>Approve</button>
                      <button type="button" className="py-2 px-4 bg-slate-700 text-slate-200 font-semibold rounded-lg hover:bg-slate-600 transition-colors" onClick={() => handleRejectPayment(e.id)}>Reject</button>
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </>
        )}

        {tab === 'matchday' && (
          <>
            {loadingList ? (
              <p className="text-slate-400">Loading...</p>
            ) : selectedMatchdayId == null ? (
              <div className="space-y-6">
                <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-6">
                  <h3 className="font-bold text-lg mb-3">Create matchday</h3>
                  <form onSubmit={handleCreateMatchday} className="flex flex-wrap items-end gap-3">
                    <div>
                      <label className="block text-sm text-slate-400 mb-1">Matchday date (Sunday; past or future allowed for backfill)</label>
                      <input type="date" value={newMatchdayDate} onChange={(e) => setNewMatchdayDate(e.target.value)} required className="rounded-lg bg-slate-800 border border-slate-600 px-3 py-2 text-slate-100" />
                    </div>
                    <button type="submit" className="py-2 px-4 bg-primary text-background-dark font-bold rounded-lg hover:shadow-[0_0_16px_rgba(10,194,71,0.3)] transition-all">Create new matchday</button>
                  </form>
                </div>
                <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-6">
                  <h3 className="font-bold text-lg mb-4">Matchdays</h3>
                  <ul className="space-y-3">
                    {matchdaysList.map((md) => (
                      <li key={md.id} className="flex flex-wrap items-center justify-between gap-3 p-4 rounded-lg bg-slate-800/50 border border-slate-700 hover:border-primary/20 transition-colors">
                        <span><strong className="text-slate-100">{md.sunday_date}</strong> <span className="text-slate-400 text-sm">– {md.status}{md.matchday_ended && ' (ended)'}{md.groups_published && ' · Groups published'}{md.fixtures_published && ' · Fixtures published'}</span></span>
                        <button type="button" className="py-2 px-4 bg-primary text-background-dark font-bold rounded-lg text-sm hover:opacity-90" onClick={() => setSelectedMatchdayId(md.id)}>Open</button>
                      </li>
                    ))}
                  </ul>
                  {matchdaysList.length === 0 && <p className="text-slate-500 py-8 text-center">No matchdays yet. Create one above.</p>}
                </div>
              </div>
            ) : matchdayData ? (
              <div className="space-y-6">
                <button type="button" className="flex items-center gap-2 py-2 px-3 rounded-lg border border-slate-600 text-slate-400 hover:bg-slate-800 hover:text-slate-100 text-sm transition-colors" onClick={() => { setSelectedMatchdayId(null); setMatchdayData(null); setMatchdayGroups(null); setMatchdayFixtures([]); setMatchdayTable([]); }}>← Back to list</button>

                <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-6">
                  <h3 className="text-xl font-bold mb-1">Matchday – {matchdayData.matchday?.sunday_date}</h3>
                  <p className="text-slate-400 text-sm mb-4">Votes: {matchdayData.vote_count ?? 0} · Status: {matchdayData.matchday?.status}</p>
                  {matchdayData.matchday?.status === 'voting_open' && (
                    <div className="flex gap-2 flex-wrap">
                      <button type="button" className="py-2 px-4 bg-primary text-background-dark font-bold rounded-lg hover:opacity-90 disabled:opacity-60" onClick={handleVoteAll} disabled={votingAll}>{votingAll ? 'Recording...' : 'Have all members vote'}</button>
                      <button type="button" className="py-2 px-4 bg-slate-700 text-slate-200 font-semibold rounded-lg hover:bg-slate-600" onClick={async () => { showToast(''); try { await closeVotingMatchday(selectedMatchdayId, getAdminToken()); showToast('Voting closed.'); fetchMatchdays(); } catch (e) { showToast(e.response?.data?.detail || 'Failed'); } }}>Close voting</button>
                    </div>
                  )}
                  {matchdayData.matchday?.status === 'closed_pending_review' && (
                    <div className="flex gap-2 flex-wrap">
                      <button type="button" className="py-2 px-4 bg-primary text-background-dark font-bold rounded-lg hover:opacity-90" onClick={handleApproveMatchday}>Approve matchday</button>
                      <button type="button" className="py-2 px-4 bg-slate-700 text-slate-200 font-semibold rounded-lg hover:bg-slate-600" onClick={handleRejectMatchday}>Reject matchday</button>
                      <button type="button" className="py-2 px-4 bg-amber-500/20 text-amber-400 font-semibold rounded-lg hover:bg-amber-500/30 border border-amber-500/40" onClick={async () => { showToast(''); try { await reopenVotingMatchday(selectedMatchdayId, getAdminToken()); showToast('Voting reopened.'); fetchMatchdays(); } catch (e) { showToast(e.response?.data?.detail || 'Failed'); } }}>Reopen voting</button>
                    </div>
                  )}
                  {matchdayData.matchday?.status === 'voting_open' && (
                    <div className="mt-3 p-3 rounded-lg bg-slate-800/50 border border-slate-700 space-y-2">
                      <p className="text-slate-400 text-sm font-medium">Manual vote</p>
                      <div className="flex flex-wrap items-center gap-2">
                        <select className="rounded-lg bg-slate-800 border border-slate-600 px-3 py-2 text-slate-100 text-sm" value="" onChange={(e) => { const v = e.target.value; if (v) { (async () => { try { await voteAddMatchday(selectedMatchdayId, Number(v), getAdminToken()); showToast('Vote added.'); fetchMatchdays(); } catch (err) { showToast(err.response?.data?.detail || 'Failed'); } })(); e.target.value = ''; } }}>
                          <option value="">Add vote for member...</option>
                          {(matchdayData.add_vote_choices || matchdayData.eligible_players || []).filter((p) => !(matchdayData.voted_players || []).some((v) => v.player_id === p.player_id)).map((p) => <option key={p.player_id} value={p.player_id}>{p.baller_name}</option>)}
                        </select>
                        {(matchdayData.add_vote_choices || matchdayData.eligible_players || []).filter((p) => !(matchdayData.voted_players || []).some((v) => v.player_id === p.player_id)).length === 0 && (matchdayData.add_vote_choices || []).length === 0 && <p className="text-amber-400 text-xs">No approved members yet. Approve members in the Approved tab first.</p>}
                        {(matchdayData.add_vote_choices || matchdayData.eligible_players || []).filter((p) => !(matchdayData.voted_players || []).some((v) => v.player_id === p.player_id)).length === 0 && (matchdayData.add_vote_choices || []).length > 0 && <p className="text-slate-500 text-xs">Everyone has already voted.</p>}
                        {(matchdayData.voted_players || []).length > 0 && (
                          <span className="text-slate-400 text-sm">Voted: {(matchdayData.voted_players || []).map((v) => (
                            <span key={v.player_id} className="inline-flex items-center gap-1 mr-2">
                              {v.baller_name}
                              <button type="button" className="text-red-400 hover:underline text-xs" onClick={async () => { try { await voteRemoveMatchday(selectedMatchdayId, v.player_id, getAdminToken()); showToast('Vote removed.'); fetchMatchdays(); } catch (err) { showToast(err.response?.data?.detail || 'Failed'); } }}>Remove</button>
                            </span>
                          ))}</span>
                        )}
                      </div>
                    </div>
                  )}
                  <div className="flex gap-2 flex-wrap mt-2">
                    <button type="button" className="py-2 px-4 bg-red-500/20 text-red-400 font-semibold rounded-lg hover:bg-red-500/30 border border-red-500/40" onClick={handleDeleteMatchday}>Delete matchday</button>
                  </div>
                </div>

                {matchdayData.matchday?.status === 'approved' && matchdayGroups?.groups?.length > 0 && (
                  <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-6">
                    {matchdayData.matchday?.groups_published ? (
                      <details open={groupsSectionOpen} onToggle={(e) => setGroupsSectionOpen(e.target.open)}>
                        <summary className="cursor-pointer font-bold mb-2 list-none flex items-center justify-between gap-2">
                          <span className="text-primary">Published groups</span>
                          <span className="text-slate-400 text-sm font-normal">(click to expand)</span>
                        </summary>
                        <div className="mt-4 pt-4 border-t border-slate-700">
                          <button type="button" className="py-2 px-4 bg-amber-500/20 text-amber-400 font-semibold rounded-lg hover:bg-amber-500/30 border border-amber-500/40 mb-2 mr-2" onClick={handleUnpublishGroups}>Unpublish groups (edit & re-publish)</button>
                          <button type="button" className="py-2 px-4 bg-slate-600 text-slate-200 font-semibold rounded-lg hover:bg-slate-500 mb-2" onClick={async () => { showToast(''); try { await regenerateMatchdayGroups(selectedMatchdayId, getAdminToken()); showToast('Groups regenerated from voters only (5+Others).'); fetchMatchdays(); } catch (e) { showToast(e.response?.data?.detail || 'Failed'); } }}>Regenerate groups (voters only, 5+Others)</button>
                          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                            {matchdayGroups.groups.map((g) => (
                              <div key={g.group_id} className="rounded-lg bg-slate-800/50 border border-slate-700 p-4">
                                <strong className="text-primary">Group {g.group_index}</strong>
                                <ul className="mt-2 space-y-2 text-sm">
                                  {g.members.map((m) => <li key={m.player_id}>{m.baller_name} #{m.jersey_number}</li>)}
                                </ul>
                              </div>
                            ))}
                          </div>
                        </div>
                      </details>
                    ) : (
                      <>
                        <h4 className="font-bold mb-2">Groups (queue moves then Apply all)</h4>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
                          {matchdayGroups.groups.map((g) => (
                            <div key={g.group_id} className="rounded-lg bg-slate-800/50 border border-slate-700 p-4">
                              <strong className="text-primary">Group {g.group_index}</strong>
                              <ul className="mt-2 space-y-2">
                                {g.members.map((m) => {
                                  const pending = pendingMoves.find((pm) => pm.player_id === m.player_id);
                                  return (
                                    <li key={m.player_id} className="flex items-center justify-between gap-2 text-sm">
                                      <span>{m.baller_name} #{m.jersey_number}</span>
                                      <span className="flex items-center gap-1">
                                        <select value={pending ? String(pending.to_group_id) : ''} onChange={(e) => { const v = e.target.value; if (v) addPendingMove(g.group_id, Number(v), m.player_id); else removePendingMove(m.player_id); }} className="rounded bg-slate-800 border border-slate-600 px-2 py-1 text-slate-100 text-xs">
                                          <option value="">Move to...</option>
                                          {matchdayGroups.groups.filter((og) => og.group_id !== g.group_id).map((og) => <option key={og.group_id} value={og.group_id}>Group {og.group_index}</option>)}
                                        </select>
                                        {pending && <button type="button" className="text-red-400 text-xs hover:underline" onClick={() => removePendingMove(m.player_id)}>Clear</button>}
                                      </span>
                                    </li>
                                  );
                                })}
                              </ul>
                            </div>
                          ))}
                        </div>
                        {pendingMoves.length > 0 && <button type="button" className="py-2 px-4 bg-primary text-background-dark font-bold rounded-lg mr-2" onClick={handleApplyBatchMoves}>Apply all moves ({pendingMoves.length})</button>}
                        <button type="button" className="py-2 px-4 bg-slate-600 text-slate-200 font-semibold rounded-lg hover:bg-slate-500 mr-2" onClick={async () => { showToast(''); try { await regenerateMatchdayGroups(selectedMatchdayId, getAdminToken()); showToast('Groups regenerated from voters only (5+Others per group).'); fetchMatchdays(); } catch (e) { showToast(e.response?.data?.detail || 'Failed'); } }}>Regenerate groups (voters only, 5+Others)</button>
                        <button type="button" className="py-2 px-4 bg-primary text-background-dark font-bold rounded-lg hover:opacity-90" onClick={handlePublishGroups}>Publish groups</button>
                      </>
                    )}
                  </div>
                )}

                {matchdayData.matchday?.status === 'approved' && matchdayData.matchday?.groups_published && (
                  <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-6">
                    <div className="flex items-center justify-between mb-3 flex-wrap gap-2">
                      <h4 className="font-bold">Attendance</h4>
                      <div className="flex items-center gap-3 text-xs text-slate-400">
                        <span className="text-primary font-bold">{attendanceSummary.present.length} present</span>
                        <span>·</span>
                        <span>{attendanceSummary.absent.length} absent</span>
                      </div>
                    </div>
                    <p className="text-slate-500 text-xs mb-4">Tap the toggle next to each player to mark them present or absent. Only present players can score/assist.</p>
                    {(attendanceSummary.present.length === 0 && attendanceSummary.absent.length === 0) ? (
                      <p className="text-amber-400 text-sm">No players in groups yet. Publish groups first, then refresh.</p>
                    ) : (
                      <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 max-h-80 overflow-y-auto custom-scrollbar pr-1">
                        {[...attendanceSummary.present.map((p) => ({ ...p, present: true })), ...attendanceSummary.absent.map((p) => ({ ...p, present: false }))]
                          .sort((a, b) => a.baller_name.localeCompare(b.baller_name))
                          .map((p) => (
                            <label
                              key={p.player_id}
                              className="flex items-center justify-between gap-3 px-3 py-2.5 rounded-lg bg-slate-800/50 border border-slate-700 hover:border-primary/30 cursor-pointer transition-colors select-none"
                            >
                              <span className={`text-sm font-medium ${p.present ? 'text-slate-100' : 'text-slate-400'}`}>{p.baller_name}</span>
                              <button
                                type="button"
                                role="switch"
                                aria-checked={p.present}
                                onClick={async () => {
                                  const token = getAdminToken();
                                  if (!token) return;
                                  try {
                                    await setMatchdayAttendanceBulk(selectedMatchdayId, [{ player_id: p.player_id, present: !p.present }], token);
                                    // optimistic update
                                    setAttendanceSummary((prev) => {
                                      if (!p.present) {
                                        return { present: [...prev.present, { player_id: p.player_id, baller_name: p.baller_name }], absent: prev.absent.filter((x) => x.player_id !== p.player_id) };
                                      } else {
                                        return { absent: [...prev.absent, { player_id: p.player_id, baller_name: p.baller_name }], present: prev.present.filter((x) => x.player_id !== p.player_id) };
                                      }
                                    });
                                  } catch (e) { showToast(e.response?.data?.detail || 'Failed'); }
                                }}
                                className={`relative inline-flex h-6 w-11 shrink-0 rounded-full border-2 border-transparent transition-colors focus:outline-none ${p.present ? 'bg-primary' : 'bg-slate-600'}`}
                              >
                                <span className={`inline-block h-5 w-5 rounded-full bg-white shadow transform transition-transform ${p.present ? 'translate-x-5' : 'translate-x-0'}`} />
                              </button>
                            </label>
                          ))}
                      </div>
                    )}
                    <button type="button" className="mt-3 text-slate-400 hover:text-slate-200 text-sm" onClick={() => { const t = getAdminToken(); if (t && selectedMatchdayId) getMatchdayAttendanceSummary(selectedMatchdayId, t).then((sum) => setAttendanceSummary({ present: sum.present || [], absent: sum.absent || [] })).catch(() => setAttendanceSummary({ present: [], absent: [] })); }}>
                      Refresh attendance
                    </button>
                  </div>
                )}

                {matchdayData.matchday?.status === 'approved' && matchdayData.matchday?.groups_published && (
                  <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-6">
                    <h4 className="font-bold mb-4">Fixtures</h4>
                    {matchdayFixtures.length === 0 ? (
                      <button type="button" className="py-2 px-4 bg-primary text-background-dark font-bold rounded-lg" onClick={async () => { showToast(''); try { await generateFixtures(selectedMatchdayId, getAdminToken()); showToast('Fixtures generated.'); fetchMatchdays(); } catch (e) { showToast(e.response?.data?.detail || 'Failed'); } }}>Generate fixtures</button>
                    ) : (
                      <>
                        {!matchdayData.matchday?.fixtures_published && <button type="button" className="py-2 px-4 bg-primary text-background-dark font-bold rounded-lg mb-4" onClick={async () => { showToast(''); try { await publishFixtures(selectedMatchdayId, getAdminToken()); showToast('Fixtures published.'); fetchMatchdays(); } catch (e) { showToast(e.response?.data?.detail || 'Failed'); } }}>Publish fixtures</button>}
                        <ul className="space-y-3">
                          {matchdayFixtures.map((f) => (
                            <li key={f.id} className="p-4 rounded-lg bg-slate-800/50 border border-slate-700 flex flex-wrap items-start gap-3">
                              <span className="font-medium">Group {f.group_a_index} vs Group {f.group_b_index} – {f.home_goals}–{f.away_goals} <span className="text-slate-400 text-sm">({f.status})</span></span>
                              {f.status === 'pending' && <button type="button" className="py-1.5 px-3 bg-primary text-background-dark font-semibold rounded-lg text-sm" onClick={async () => { try { await startFixture(selectedMatchdayId, f.id, getAdminToken()); fetchMatchdays(); } catch (e) { showToast(e.response?.data?.detail || 'Failed'); } }}>Start</button>}
                              {(f.status === 'in_progress' || f.status === 'completed') && (
                                <>
                                  <button type="button" className="py-1.5 px-3 bg-slate-600 text-slate-100 font-semibold rounded-lg text-sm" onClick={() => { const open = addGoalFixtureId !== f.id; setAddGoalFixtureId(open ? f.id : null); if (open) { setAddGoalScorer(''); setAddGoalAssister(''); } }}>Add goal</button>
                                  {f.status === 'in_progress' && (
                                    <>
                                      <button type="button" className="py-1.5 px-3 bg-amber-500/20 text-amber-400 font-semibold rounded-lg text-sm hover:bg-amber-500/30" onClick={() => { setAddCardFixtureId(addCardFixtureId === f.id ? null : f.id); setAddCardType('yellow'); setAddCardPlayerId(''); }}>+ Yellow</button>
                                      <button type="button" className="py-1.5 px-3 bg-red-500/20 text-red-400 font-semibold rounded-lg text-sm hover:bg-red-500/30" onClick={() => { setAddCardFixtureId(addCardFixtureId === f.id ? null : f.id); setAddCardType('red'); setAddCardPlayerId(''); }}>+ Red</button>
                                      <button type="button" className="py-1.5 px-3 bg-slate-600 text-slate-100 font-semibold rounded-lg text-sm" onClick={async () => { try { await endFixture(selectedMatchdayId, f.id, getAdminToken()); fetchMatchdays(); } catch (e) { showToast(e.response?.data?.detail || 'Failed'); } }}>End fixture</button>
                                    </>
                                  )}
                                </>
                              )}
                              {(f.status === 'in_progress' || f.status === 'completed') && (f.goals?.length > 0) && (
                                <div className="w-full mt-2 flex flex-wrap items-center gap-2">
                                  <span className="text-slate-400 text-sm">Goals:</span>
                                  {(f.goals || []).map((g) => (
                                    <span key={g.id} className="inline-flex items-center gap-1.5 py-1 px-2 rounded bg-slate-700/80 text-slate-200 text-sm">
                                      {g.scorer_name}{g.assister_name ? ` (assist: ${g.assister_name})` : ''}
                                      <button type="button" className="text-red-400 hover:underline text-xs font-medium" onClick={async () => { try { await removeGoal(selectedMatchdayId, f.id, g.id, getAdminToken()); showToast('Goal removed.'); refreshFixtures(selectedMatchdayId); } catch (e) { showToast(e.response?.data?.detail || 'Failed'); } }}>Remove</button>
                                    </span>
                                  ))}
                                </div>
                              )}
                              {addGoalFixtureId === f.id && (f.status === 'in_progress' || f.status === 'completed') && (
                                <div className="w-full mt-3 p-4 rounded-lg bg-slate-800 border border-slate-600 flex flex-col gap-2 max-w-xs">
                                  <label className="text-sm text-slate-400">Scorer (goal goes to scorer&apos;s team)</label>
                                  <select value={addGoalScorer === '' || addGoalScorer == null ? '' : String(addGoalScorer)} onChange={(e) => setAddGoalScorer(e.target.value === '' ? '' : Number(e.target.value))} className="rounded-lg bg-slate-800 border border-slate-600 px-3 py-2 text-slate-100 text-sm">
                                    <option value="">Select scorer</option>
                                    {(f.goal_choices || []).map((c) => <option key={c.id} value={c.id}>{c.baller_name}</option>)}
                                  </select>
                                  <label className="text-sm text-slate-400">Assister (optional)</label>
                                  <select value={addGoalAssister === '' || addGoalAssister == null ? '' : String(addGoalAssister)} onChange={(e) => setAddGoalAssister(e.target.value === '' ? '' : Number(e.target.value))} className="rounded-lg bg-slate-800 border border-slate-600 px-3 py-2 text-slate-100 text-sm">
                                    <option value="">None</option>
                                    {(f.goal_choices || []).map((c) => <option key={c.id} value={c.id}>{c.baller_name}</option>)}
                                  </select>
                                  <button type="button" className="py-2 px-4 bg-primary text-background-dark font-bold rounded-lg text-sm" onClick={async () => { const sid = addGoalScorer === '' ? null : Number(addGoalScorer); if (sid == null) { showToast('Select a scorer.'); return; } try { await addGoal(selectedMatchdayId, f.id, { scorer_player_id: sid, assister_player_id: addGoalAssister === '' ? null : Number(addGoalAssister) }, getAdminToken()); setAddGoalFixtureId(null); setAddGoalScorer(''); setAddGoalAssister(''); showToast(''); refreshFixtures(selectedMatchdayId); } catch (e) { showToast(e.response?.data?.detail || 'Failed'); } }}>Add goal</button>
                                </div>
                              )}
                              {f.status === 'in_progress' && addCardFixtureId === f.id && addCardType && (
                                <div className="w-full mt-3 p-4 rounded-lg bg-slate-800 border border-slate-600 flex flex-col gap-2 max-w-xs">
                                  <label className="text-sm text-slate-400">{addCardType === 'yellow' ? 'Yellow' : 'Red'} card – select player</label>
                                  <select value={addCardPlayerId === '' ? '' : String(addCardPlayerId)} onChange={(e) => setAddCardPlayerId(e.target.value === '' ? '' : Number(e.target.value))} className="rounded-lg bg-slate-800 border border-slate-600 px-3 py-2 text-slate-100 text-sm">
                                    <option value="">Select player</option>
                                    {(f.goal_choices || []).map((c) => <option key={c.id} value={c.id}>{c.baller_name}</option>)}
                                  </select>
                                  <div className="flex gap-2">
                                    <button type="button" className="py-2 px-4 bg-primary text-background-dark font-bold rounded-lg text-sm" onClick={async () => { if (!addCardPlayerId) { showToast('Select a player.'); return; } try { await addMatchdayCard(selectedMatchdayId, { player_id: addCardPlayerId, card_type: addCardType, fixture_id: f.id }, getAdminToken()); setAddCardFixtureId(null); setAddCardType(null); setAddCardPlayerId(''); showToast(addCardType === 'yellow' ? 'Yellow card added.' : 'Red card added.'); refreshFixtures(selectedMatchdayId); } catch (e) { showToast(e.response?.data?.detail || 'Failed'); } }}>Confirm</button>
                                    <button type="button" className="py-2 px-4 bg-slate-600 text-slate-100 rounded-lg text-sm" onClick={() => { setAddCardFixtureId(null); setAddCardType(null); setAddCardPlayerId(''); }}>Cancel</button>
                                  </div>
                                </div>
                              )}
                            </li>
                          ))}
                        </ul>
                        {!matchdayData.matchday?.matchday_ended && <button type="button" className="mt-4 py-2 px-4 bg-slate-700 text-slate-200 font-semibold rounded-lg hover:bg-slate-600" onClick={async () => { showToast(''); try { await endMatchday(selectedMatchdayId, getAdminToken()); showToast('Matchday ended.'); fetchMatchdays(); } catch (e) { showToast(e.response?.data?.detail || 'Failed'); } }}>End matchday</button>}
                        {matchdayData.matchday?.matchday_ended && <button type="button" className="mt-4 py-2 px-4 bg-amber-600 text-white font-semibold rounded-lg hover:bg-amber-500" onClick={async () => { showToast(''); try { await reopenMatchday(selectedMatchdayId, getAdminToken()); showToast('Matchday reopened. End it again to refresh leaderboard/stats.'); fetchMatchdays(); } catch (e) { showToast(e.response?.data?.detail || 'Failed'); } }}>Reopen matchday</button>}
                      </>
                    )}
                  </div>
                )}

                {matchdayTable.length > 0 && (
                  <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-6 overflow-x-auto">
                    <h4 className="font-bold mb-4">League table</h4>
                    <table className="w-full text-sm">
                      <thead><tr className="text-slate-400 border-b border-slate-700"><th className="text-left py-2 px-2">Group</th><th className="text-center py-2 px-2">P</th><th className="text-center py-2 px-2">W</th><th className="text-center py-2 px-2">D</th><th className="text-center py-2 px-2">L</th><th className="text-center py-2 px-2">GF</th><th className="text-center py-2 px-2">GA</th><th className="text-center py-2 px-2 font-bold text-primary">Pts</th></tr></thead>
                      <tbody>
                        {matchdayTable.map((row) => (
                          <tr key={row.group_id} className="border-b border-slate-700/50 hover:bg-primary/5"><td className="py-2 px-2">Group {row.group_index}</td><td className="py-2 px-2 text-center">{row.played}</td><td className="py-2 px-2 text-center">{row.won}</td><td className="py-2 px-2 text-center">{row.drawn}</td><td className="py-2 px-2 text-center">{row.lost}</td><td className="py-2 px-2 text-center">{row.goals_for}</td><td className="py-2 px-2 text-center">{row.goals_against}</td><td className="py-2 px-2 text-center font-bold text-primary">{row.points}</td></tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}

                {matchdayPlayerRatings.length > 0 && (
                  <div className="bg-slate-900/40 border border-primary/10 rounded-xl p-6 overflow-x-auto">
                    <h4 className="font-bold mb-4">Player ratings (this matchday)</h4>
                    <p className="text-slate-400 text-sm mb-3">Updates as fixtures complete. Before any fixture, present players all show 5.0.</p>
                    <table className="w-full text-sm">
                      <thead><tr className="text-slate-400 border-b border-slate-700"><th className="text-left py-2 px-2">#</th><th className="text-left py-2 px-2">Player</th><th className="text-left py-2 px-2">Group</th><th className="text-right py-2 px-2 font-bold text-primary">Rating</th></tr></thead>
                      <tbody>
                        {(() => {
                          const hasCompletedFixture = matchdayFixtures.some((f) => f.status === 'completed');
                          return matchdayPlayerRatings.map((row, idx) => (
                            <tr key={row.player_id} className="border-b border-slate-700/50 hover:bg-primary/5">
                              <td className="py-2 px-2 text-slate-500">{idx + 1}</td>
                              <td className="py-2 px-2">{row.baller_name} #{row.jersey_number}</td>
                              <td className="py-2 px-2">Group {row.group_index}</td>
                              <td className="py-2 px-2 text-right font-bold text-primary">
                                {hasCompletedFixture ? row.rating : <span className="text-slate-500">—</span>}
                              </td>
                            </tr>
                          ));
                        })()}
                      </tbody>
                    </table>
                  </div>
                )}

                {matchdayData.matchday?.status === 'approved' && (!matchdayGroups?.groups || matchdayGroups.groups.length === 0) && <p className="text-slate-400 text-sm">Approve matchday to create groups, then refresh.</p>}
              </div>
            ) : (
              <p className="text-slate-500">Loading matchday...</p>
            )}
          </>
        )}
        </div>
      </main>
      </div>
    </div>
  );
}
