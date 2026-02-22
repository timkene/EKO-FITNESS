import axios from 'axios';

const baseURL = import.meta.env.VITE_FOOTBALL_API_URL || 'http://localhost:8000/api/v1/football';

export const footballApi = axios.create({
  baseURL,
  headers: { 'Content-Type': 'application/json' },
  timeout: 60000, // Render cold start can take 30–60s
});

export async function signup(data) {
  const res = await footballApi.post('/signup', data);
  return res.data;
}

export async function login(username, password) {
  const res = await footballApi.post('/login', { username, password });
  return res.data;
}

export async function adminLogin(username, password) {
  const res = await footballApi.post('/admin/login', { username, password });
  return res.data;
}

export async function getPending(token) {
  const res = await footballApi.get('/admin/pending', {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.data;
}

export async function approvePlayer(playerId, token) {
  const res = await footballApi.post(`/admin/approve/${playerId}`, null, {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.data;
}

export async function rejectPlayer(playerId, token) {
  const res = await footballApi.post(`/admin/reject/${playerId}`, null, {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.data;
}

export async function getApproved(token) {
  const res = await footballApi.get('/admin/approved', {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.data;
}

export async function suspendPlayer(playerId, token) {
  const res = await footballApi.post(`/admin/suspend/${playerId}`, null, {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.data;
}

export async function activatePlayer(playerId, token) {
  const res = await footballApi.post(`/admin/activate/${playerId}`, null, {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.data;
}

export async function deletePlayer(playerId, token) {
  const res = await footballApi.delete(`/admin/players/${playerId}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.data;
}

export async function setDues(playerId, year, quarter, status, token, waiverDueBy = null) {
  const body = { year, quarter, status };
  if (waiverDueBy) body.waiver_due_by = waiverDueBy;
  const res = await footballApi.put(`/admin/dues/${playerId}`, body, {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.data;
}

/** Get dues by quarter for admin: all approved members with status for that year/quarter. */
export async function getDuesByQuarter(token, year, quarter) {
  const res = await footballApi.get('/admin/dues-by-quarter', {
    params: { year, quarter },
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.data;
}

export async function getPaymentEvidence(token) {
  const res = await footballApi.get('/admin/payment-evidence', {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.data;
}

/** Fetch payment evidence file as blob; use with URL.createObjectURL(blob) to view in new tab */
export async function getPaymentEvidenceFile(evidenceId, token) {
  const res = await footballApi.get(`/admin/payment-evidence/${evidenceId}/file`, {
    headers: { Authorization: `Bearer ${token}` },
    responseType: 'blob',
  });
  return res.data;
}

export async function approvePayment(evidenceId, token) {
  const res = await footballApi.post(`/admin/approve-payment/${evidenceId}`, null, {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.data;
}

export async function rejectPayment(evidenceId, token) {
  const res = await footballApi.post(`/admin/reject-payment/${evidenceId}`, null, {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.data;
}

export async function getMemberDues(token) {
  const res = await footballApi.get('/member/dues', {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.data;
}

export async function submitPaymentEvidence(file, token) {
  const form = new FormData();
  form.append('file', file);
  const res = await footballApi.post('/member/payment-evidence', form, {
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'multipart/form-data' },
  });
  return res.data;
}

export async function applyWaiver(dueBy, token) {
  const res = await footballApi.post('/member/waiver', { due_by: dueBy }, {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.data;
}

// ----- Matchday module (by id) -----
export async function listAdminMatchdays(token) {
  const res = await footballApi.get('/admin/matchdays', { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function createMatchday(matchdayDate, token) {
  const res = await footballApi.post('/admin/matchdays', { matchday_date: matchdayDate }, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function getAdminMatchday(matchdayId, token) {
  const res = await footballApi.get(`/admin/matchdays/${matchdayId}`, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function listMemberMatchdays(token) {
  const res = await footballApi.get('/member/matchdays', { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function getMemberMatchday(matchdayId, token) {
  const res = await footballApi.get(`/member/matchdays/${matchdayId}`, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function voteMatchday(matchdayId, token) {
  const res = await footballApi.post(`/member/matchdays/${matchdayId}/vote`, null, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function voteAllMatchday(matchdayId, token) {
  const res = await footballApi.post(`/admin/matchdays/${matchdayId}/vote-all`, null, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function closeVotingMatchday(matchdayId, token) {
  const res = await footballApi.post(`/admin/matchdays/${matchdayId}/close-voting`, null, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function reopenVotingMatchday(matchdayId, token) {
  const res = await footballApi.post(`/admin/matchdays/${matchdayId}/reopen-voting`, null, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function voteAddMatchday(matchdayId, playerId, token) {
  const res = await footballApi.post(`/admin/matchdays/${matchdayId}/vote-add`, { player_id: playerId }, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function voteRemoveMatchday(matchdayId, playerId, token) {
  const res = await footballApi.post(`/admin/matchdays/${matchdayId}/vote-remove`, { player_id: playerId }, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function approveMatchday(matchdayId, token) {
  const res = await footballApi.post(`/admin/matchdays/${matchdayId}/approve`, null, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function rejectMatchday(matchdayId, token) {
  const res = await footballApi.post(`/admin/matchdays/${matchdayId}/reject`, null, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function deleteMatchday(matchdayId, token) {
  const res = await footballApi.delete(`/admin/matchdays/${matchdayId}`, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function getAdminMatchdayGroups(matchdayId, token) {
  const res = await footballApi.get(`/admin/matchdays/${matchdayId}/groups`, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function moveMatchdayMember(matchdayId, body, token) {
  const res = await footballApi.put(`/admin/matchdays/${matchdayId}/groups/move`, body, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function moveMatchdayMemberBatch(matchdayId, moves, token) {
  const res = await footballApi.put(`/admin/matchdays/${matchdayId}/groups/move-batch`, { moves }, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function unpublishMatchdayGroups(matchdayId, token) {
  const res = await footballApi.post(`/admin/matchdays/${matchdayId}/groups/unpublish`, null, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function regenerateMatchdayGroups(matchdayId, token) {
  const res = await footballApi.post(`/admin/matchdays/${matchdayId}/groups/regenerate`, null, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function publishMatchdayGroups(matchdayId, token) {
  const res = await footballApi.post(`/admin/matchdays/${matchdayId}/groups/publish`, null, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function getMatchdayAttendance(matchdayId, token) {
  const res = await footballApi.get(`/admin/matchdays/${matchdayId}/attendance`, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function setMatchdayAttendance(matchdayId, body, token) {
  const res = await footballApi.put(`/admin/matchdays/${matchdayId}/attendance`, body, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function setMatchdayAttendanceBulk(matchdayId, updates, token) {
  const res = await footballApi.put(`/admin/matchdays/${matchdayId}/attendance/bulk`, { updates }, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function getMatchdayAttendanceSummary(matchdayId, token) {
  const res = await footballApi.get(`/admin/matchdays/${matchdayId}/attendance/summary`, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function getMatchdayCards(matchdayId, token) {
  const res = await footballApi.get(`/admin/matchdays/${matchdayId}/cards`, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function addMatchdayCard(matchdayId, body, token) {
  const res = await footballApi.post(`/admin/matchdays/${matchdayId}/cards`, body, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function getFixtureCards(matchdayId, fixtureId, token) {
  const res = await footballApi.get(`/admin/matchdays/${matchdayId}/fixtures/${fixtureId}/cards`, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function generateFixtures(matchdayId, token) {
  const res = await footballApi.post(`/admin/matchdays/${matchdayId}/fixtures/generate`, null, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function getFixtures(matchdayId, token) {
  const res = await footballApi.get(`/admin/matchdays/${matchdayId}/fixtures`, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function publishFixtures(matchdayId, token) {
  const res = await footballApi.post(`/admin/matchdays/${matchdayId}/fixtures/publish`, null, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function startFixture(matchdayId, fixtureId, token) {
  const res = await footballApi.post(`/admin/matchdays/${matchdayId}/fixtures/${fixtureId}/start`, null, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function addGoal(matchdayId, fixtureId, body, token) {
  const res = await footballApi.post(`/admin/matchdays/${matchdayId}/fixtures/${fixtureId}/goals`, body, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function removeGoal(matchdayId, fixtureId, goalId, token) {
  const res = await footballApi.delete(`/admin/matchdays/${matchdayId}/fixtures/${fixtureId}/goals/${goalId}`, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function getFixtureGoals(matchdayId, fixtureId, token) {
  const res = await footballApi.get(`/admin/matchdays/${matchdayId}/fixtures/${fixtureId}/goals`, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function endFixture(matchdayId, fixtureId, token) {
  const res = await footballApi.post(`/admin/matchdays/${matchdayId}/fixtures/${fixtureId}/end`, null, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function endMatchday(matchdayId, token) {
  const res = await footballApi.post(`/admin/matchdays/${matchdayId}/end-matchday`, null, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function getMatchdayTable(matchdayId, token) {
  const res = await footballApi.get(`/admin/matchdays/${matchdayId}/table`, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function getMatchdayPlayerRatings(matchdayId, token) {
  const res = await footballApi.get(`/admin/matchdays/${matchdayId}/player-ratings`, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function getMemberMatchdayTable(matchdayId, token) {
  const res = await footballApi.get(`/member/matchdays/${matchdayId}/table`, { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function getMemberStats(token) {
  const res = await footballApi.get('/member/stats', { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function getMemberLeaderboard(token) {
  const res = await footballApi.get('/member/leaderboard', { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}

export async function getMemberTopFiveBallers(token) {
  const res = await footballApi.get('/member/top-five-ballers', { headers: { Authorization: `Bearer ${token}` } });
  return res.data;
}
