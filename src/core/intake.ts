/** Trigger ingestion, thread correlation, subscription fan-out, and evaluation queuing. */
import { key } from './ids.ts'
import { isoTime, liveTicket, threadKey } from './model.ts'
import type { Ticket, Trigger } from './model.ts'
import { EnginePart, type EngineOptions } from './part.ts'
import { ticketStage } from './stage.ts'
import type { Tickets } from './tickets.ts'

export type ReceiveResult = 'stored' | 'duplicate' | 'filtered'

/** The notice that tells a person their message reached a ticket that is not working on it right now, so a queued wait is not silence. */
export const QUEUED_NOTICE = '收到，已记在当前任务上；轮到这条时会在原话题回复。'

/**
 * Ingests incoming triggers from Sources, deduplicating and filtering them against SourceScope before recording thread correlations.
 * Fans listened triggers out to subscribed tickets via Tickets and queues for Eval, on its primary thread, every trigger no
 * ticket's worker decides (see `followupOwners`).
 * Engine delegates trigger reception to this part.
 */
export class Intake extends EnginePart {
  constructor(options: EngineOptions, private readonly tickets: Tickets) { super(options) }

  receive(received: Trigger): ReceiveResult {
    // Normalize source timestamps (such as GitHub whole seconds) to canonical ISO form.
    const trigger = { ...received, at: isoTime(received.at) }
    if (!trigger.threads.length) throw new Error('A trigger must belong to at least one thread')
    if (this.repo.triggers.get(trigger.id)) return 'duplicate'
    const at = this.now()
    const filter = this.options.scope.scope(trigger)
    if (!filter.listened) {
      this.repo.triggers.save(trigger); this.repo.triggers.saveDisposition(trigger.id, { filtered: { code: filter.reason }, fanout: [], at })
      return 'filtered'
    }
    this.repo.triggers.save(trigger)
    const keys = trigger.threads.map(threadKey)
    // Track reached tickets and whether they were reached only via context threads (a ticket's own thread makes `context` false).
    const reached = new Map<string, { ticket: Ticket; context: boolean }>()
    for (const ref of trigger.threads) {
      const thread = this.thread(ref)
      if (!thread.lastAt || trigger.at > thread.lastAt) thread.lastAt = trigger.at
      thread.correlated = [...new Set([...thread.correlated, ...keys.filter(other => other !== thread.key)])]
      for (const ticketId of thread.subscriptions) {
        const ticket = this.repo.tickets.get(ticketId)
        // Only a person's message on a non-context thread resumes an ended ticket; bots and CI do not (the Executor re-reads
        // threads when it resumes), and a human-stopped ticket receives nothing because its Mandate was withdrawn.
        const followed = !!thread.context?.includes(ticketId)
        const resumes = trigger.human && ticket?.ended?.by !== 'human' && !followed
        if (!ticket || (!liveTicket(ticket) && !resumes)) continue
        reached.set(ticketId, { ticket, context: followed && (reached.get(ticketId)?.context ?? true) })
      }
      this.repo.threads.save(thread)
    }
    for (const [ticketId, { ticket, context: followed }] of reached) {
      this.tickets.addInput(ticket, { id: key('input', ticketId, 'trigger', trigger.id), kind: 'trigger', triggerId: trigger.id, at,
        ...trigger.human ? { human: true } : {}, ...followed ? { context: true } : {} })
    }
    const fanout = [...reached.keys()], context = fanout.filter(id => reached.get(id)!.context)
    const disposition = { fanout, ...context.length ? { context } : {}, at }
    this.repo.triggers.saveDisposition(trigger.id, disposition)
    // A person's message that no ticket decides, such as one that reached only tickets a person stopped or that follow the
    // Thread for context, and every bot's, goes to Eval.
    if (!this.owners(trigger).length) this.queueForEval(trigger)
    for (const ticketId of fanout) this.tickets.wake(ticketId)
    if (trigger.human) this.acknowledgeQueued(trigger, reached)
    this.emit('trigger.received', { triggerId: trigger.id, threads: trigger.threads.map(threadKey) })
    return 'stored'
  }
  /**
   * A person's message that reaches a live ticket with no worker turn coming (its stage is `person` or `idle`) is acknowledged
   * before the Executor gets to it: it may wait in the Executor's queue behind other work, and until the worker takes it up the
   * person would see silence. One notice per message and ticket, kept by the Outbox under a deterministic id, so a worker's own
   * acknowledgement or answer that follows neither repeats it nor is suppressed by it.
   */
  private acknowledgeQueued(trigger: Trigger, reached: Map<string, { ticket: Ticket; context: boolean }>): void {
    const { channel } = trigger
    if (!channel) return
    for (const [ticketId, { ticket }] of reached) {
      const current = this.repo.tickets.get(ticketId) ?? ticket
      if (!liveTicket(current) || ticketStage(current).stage === 'working') continue
      const id = key('queued-notice', trigger.id, ticketId)
      if (this.options.outbox.status(id)) continue
      this.options.outbox.send({ id, channel, text: QUEUED_NOTICE, kind: 'notice', ticketId })
    }
  }
}
