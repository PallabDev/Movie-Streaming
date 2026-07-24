import { Router } from 'express';
import { requireAdmin } from '../middleware/auth.js';
import { db } from '../db/index.js';
import { users, streams } from '../db/schema.js';
import { eq, sql, desc, count, sum } from 'drizzle-orm';

const router = Router();

router.get('/', requireAdmin, async (req, res) => {
  const allUsers = await db.select().from(users).orderBy(desc(users.createdAt));

  const totalStreams = await db.select({ count: sql`count(*)::int` }).from(streams);
  const totalHours = await db.select({
    total: sql`coalesce(sum(${streams.duration}), 0)::float / 3600`,
  }).from(streams);
  const activeCount = await db.select({ count: sql`count(*)::int` }).from(streams).where(eq(streams.status, 'active'));

  const recentStreams = await db
    .select({
      id: streams.id,
      title: streams.title,
      status: streams.status,
      duration: streams.duration,
      createdAt: streams.createdAt,
      userName: users.name,
      userEmail: users.email,
    })
    .from(streams)
    .leftJoin(users, eq(streams.userId, users.id))
    .orderBy(desc(streams.createdAt))
    .limit(20);

  res.render('admin', {
    title: 'Admin Panel',
    allUsers,
    stats: {
      totalUsers: allUsers.length,
      totalStreams: totalStreams[0].count,
      totalHours: (totalHours[0].total || 0).toFixed(1),
      activeStreams: activeCount[0].count,
    },
    recentStreams,
    user: req.session.user,
  });
});

router.post('/user/:id/grant', requireAdmin, async (req, res) => {
  try {
    await db.update(users).set({ hasAccess: true }).where(eq(users.id, req.params.id));
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false });
  }
});

router.post('/user/:id/revoke', requireAdmin, async (req, res) => {
  try {
    await db.update(users).set({ hasAccess: false }).where(eq(users.id, req.params.id));
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false });
  }
});

router.post('/user/:id/role', requireAdmin, async (req, res) => {
  try {
    const [user] = await db.select().from(users).where(eq(users.id, req.params.id));
    if (!user) return res.status(404).json({ success: false });
    const newRole = user.role === 'admin' ? 'user' : 'admin';
    await db.update(users).set({ role: newRole }).where(eq(users.id, req.params.id));
    res.json({ success: true, role: newRole });
  } catch (err) {
    res.status(500).json({ success: false });
  }
});

router.post('/user/:id/limit', requireAdmin, async (req, res) => {
  try {
    const { limit } = req.body;
    await db.update(users).set({ streamLimit: parseInt(limit) }).where(eq(users.id, req.params.id));
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false });
  }
});

router.post('/user/:id/reset', requireAdmin, async (req, res) => {
  try {
    await db.update(users).set({ streamsUsed: 0, lastResetAt: new Date() }).where(eq(users.id, req.params.id));
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false });
  }
});

router.post('/user/:id/delete', requireAdmin, async (req, res) => {
  try {
    if (req.params.id === req.session.user.id) {
      return res.status(400).json({ success: false, message: 'Cannot delete yourself' });
    }
    await db.delete(users).where(eq(users.id, req.params.id));
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false });
  }
});

export default router;
