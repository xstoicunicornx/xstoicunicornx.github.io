export const TYPES = ['review', 'pr']

// Workflow order, used by the status dropdown.
export const STATUSES = ['todo', 'queued', 'active', 'done', 'cancelled']

// Order of the sections on a project page.
export const SECTION_ORDER = ['active', 'queued', 'todo', 'done', 'cancelled']

export const STATUS_LABELS = {
  todo: 'Todo',
  queued: 'Queued',
  active: 'Active',
  done: 'Done',
  cancelled: 'Cancelled'
}

// The date field set when an item enters a status.
export const STATUS_DATE_FIELDS = {
  active: 'activated',
  done: 'completed',
  cancelled: 'cancelled'
}

// The status the advance button moves to. Done and cancelled items get archived instead.
export const NEXT_STATUS = {
  todo: 'queued',
  queued: 'active',
  active: 'done'
}
