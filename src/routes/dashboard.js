import { Router } from 'express';
import { requireAuth } from '../middleware/auth.js';
import { db } from '../db/index.js';
import { streams, users } from '../db/schema.js';
import { eq, desc, sql, and, gte } from 'drizzle-orm';
import crypto from 'crypto';

const router = Router();

router.get('/', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const userStreams = await db
    .select()
    .from(streams)
    .where(eq(streams.userId, userId))
    .orderBy(desc(streams.createdAt));

  const [activeStream] = await db
    .select()
    .from(streams)
    .where(and(eq(streams.userId, userId), eq(streams.status, 'active')));

  const [currentUser] = await db
    .select()
    .from(users)
    .where(eq(users.id, userId));

  req.session.user.streamsUsed = currentUser.streamsUsed;
  req.session.user.streamLimit = currentUser.streamLimit;

  const anyStreamActive = await db
    .select({ count: sql`count(*)::int` })
    .from(streams)
    .where(eq(streams.status, 'active'));

  const isSomeoneStreaming = anyStreamActive[0].count > 0 && !activeStream;

  res.render('dashboard', {
    title: 'Dashboard',
    userStreams,
    activeStream,
    isSomeoneStreaming,
    user: req.session.user,
  });
});

router.post('/stream/create', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const { title } = req.body;

  try {
    const [currentUser] = await db
      .select()
      .from(users)
      .where(eq(users.id, userId));

    if (!currentUser.hasAccess) {
      return res.status(403).json({ success: false, message: 'No access' });
    }

    const anyActive = await db
      .select({ count: sql`count(*)::int` })
      .from(streams)
      .where(eq(streams.status, 'active'));

    if (anyActive[0].count > 0) {
      return res.status(400).json({ success: false, message: 'Someone is already streaming. Please wait.' });
    }

    const now = new Date();
    const lastReset = new Date(currentUser.lastResetAt);
    if (now.getMonth() !== lastReset.getMonth() || now.getFullYear() !== lastReset.getFullYear()) {
      await db.update(users).set({ streamsUsed: 0, lastResetAt: now }).where(eq(users.id, userId));
      currentUser.streamsUsed = 0;
    }

    if (currentUser.streamsUsed >= currentUser.streamLimit) {
      return res.status(400).json({ success: false, message: 'Monthly stream limit reached.' });
    }

    const streamKey = crypto.randomBytes(8).toString('hex');
    const [stream] = await db
      .insert(streams)
      .values({ title: title || 'Untitled Stream', userId, streamKey })
      .returning();

    res.json({ success: true, streamId: stream.id });
  } catch (err) {
    console.error('Create stream error:', err);
    res.status(500).json({ success: false, message: 'Failed to create stream' });
  }
});

router.post('/stream/:id/delete', requireAuth, async (req, res) => {
  const userId = req.session.user.id;
  const streamId = req.params.id;
  try {
    await db.delete(streams).where(and(eq(streams.id, streamId), eq(streams.userId, userId)));
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false });
  }
});

export default router;
