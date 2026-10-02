"use client";

import { createContext, useCallback, useContext, useEffect, useRef, useState, type ReactNode } from "react";

import { copyText, useFlag } from "@/lib/hooks";
import { AlertCircleIcon, CheckIcon, CloseIcon, CopyIcon, WarningIcon } from "./icons";

// ---- toasts ----

type Toast = { id: number; text: string; tone: "ok" | "error" };
type Notify = (text: string, tone?: Toast["tone"]) => void;

const ToastContext = createContext<Notify>(() => {});

export function useToast(): Notify {
  return useContext(ToastContext);
}

export function ToastProvider({ children }: { children: ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const next = useRef(0);
  const notify = useCallback<Notify>((text, tone = "ok") => {
    const id = (next.current += 1);
    setToasts((all) => [...all.slice(-3), { id, text, tone }]);
    window.setTimeout(() => setToasts((all) => all.filter((t) => t.id !== id)), tone === "error" ? 8000 : 4000);
  }, []);
  return (
    <ToastContext.Provider value={notify}>
      {children}
      <div className="toasts" role="status" aria-live="polite">
        {toasts.map((t) => (
          <div key={t.id} className={`toast${t.tone === "error" ? " toast-error" : ""}`}>
            {t.tone === "error" ? (
              <AlertCircleIcon size={16} color="var(--danger-text)" />
            ) : (
              <CheckIcon size={16} color="var(--ok-text)" />
            )}
            <span>{t.text}</span>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}

// ---- dialogs ----

/** A modal `<dialog>`: focus is trapped and Escape closes it. */
export function Dialog({
  open,
  onClose,
  labelledBy,
  children,
}: {
  open: boolean;
  onClose: () => void;
  labelledBy: string;
  children: ReactNode;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const dialog = ref.current;
    if (!dialog) return;
    if (open && !dialog.open) dialog.showModal();
    if (!open && dialog.open) dialog.close();
  }, [open]);
  return (
    <dialog
      ref={ref}
      className="dialog"
      aria-labelledby={labelledBy}
      onCancel={(e) => {
        e.preventDefault();
        onClose();
      }}
    >
      {open && children}
    </dialog>
  );
}

export function DialogHead({ id, title, sub, onClose }: { id: string; title: string; sub?: ReactNode; onClose: () => void }) {
  return (
    <div className="dialog-head">
      <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
        <h2 id={id} className="dialog-title">
          {title}
        </h2>
        {sub && <span style={{ fontSize: 13, color: "var(--subtle)" }}>{sub}</span>}
      </div>
      <button type="button" className="btn btn-quiet btn-icon" aria-label="Close" onClick={onClose}>
        <CloseIcon />
      </button>
    </div>
  );
}

/** Asks before something that cannot be undone. */
export function ConfirmDialog({
  open,
  title,
  body,
  confirm,
  onConfirm,
  onClose,
}: {
  open: boolean;
  title: string;
  body: ReactNode;
  confirm: string;
  onConfirm: () => Promise<void> | void;
  onClose: () => void;
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [wasOpen, setWasOpen] = useState(open);
  if (wasOpen !== open) {
    setWasOpen(open);
    if (open) setError(null);
  }
  return (
    <Dialog open={open} onClose={onClose} labelledBy="confirm-title">
      <DialogHead id="confirm-title" title={title} onClose={onClose} />
      <div className="dialog-body">
        <div style={{ color: "var(--muted)" }}>{body}</div>
        {error && <p className="error-text">{error}</p>}
      </div>
      <div className="dialog-foot">
        <button type="button" className="btn" onClick={onClose}>
          Cancel
        </button>
        <button
          type="button"
          className="btn btn-danger"
          disabled={busy}
          onClick={async () => {
            setBusy(true);
            try {
              await onConfirm();
              onClose();
            } catch (e) {
              setError(e instanceof Error ? e.message : String(e));
            } finally {
              setBusy(false);
            }
          }}
        >
          {busy ? "Working…" : confirm}
        </button>
      </div>
    </Dialog>
  );
}

// ---- small pieces ----

export function CopyButton({ text, label, size = "sm" }: { text: string; label: string; size?: "sm" | "xs" }) {
  const [copied, mark] = useFlag();
  const notify = useToast();
  return (
    <button
      type="button"
      className={`btn btn-quiet btn-icon btn-${size}`}
      aria-label={copied ? "Copied" : label}
      title={copied ? "Copied" : label}
      onClick={async () => {
        if (await copyText(text)) mark();
        else notify("Copying is blocked in this browser; select the text instead.", "error");
      }}
    >
      {copied ? <CheckIcon size={14} color="var(--ok-text)" /> : <CopyIcon size={14} />}
    </button>
  );
}

export function Switch({
  checked,
  label,
  onChange,
  disabled,
}: {
  checked: boolean;
  label: string;
  onChange: (on: boolean) => void;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      role="switch"
      className="switch"
      aria-checked={checked}
      aria-label={label}
      disabled={disabled}
      onClick={() => onChange(!checked)}
    >
      <span />
    </button>
  );
}

export function Notice({ title, children, tone = "warn" }: { title: string; children?: ReactNode; tone?: "warn" | "info" }) {
  return (
    <div className={`notice${tone === "info" ? " notice-info" : ""}`}>
      <WarningIcon size={18} color={tone === "warn" ? "var(--warn)" : "var(--muted)"} />
      <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <span style={{ fontWeight: 500 }}>{title}</span>
        {children && <span style={{ color: "var(--muted)" }}>{children}</span>}
      </div>
    </div>
  );
}

export function ErrorPanel({ message, onRetry }: { message: string; onRetry?: () => void }) {
  return (
    <div className="empty" role="alert">
      <div className="empty-icon">
        <AlertCircleIcon size={18} />
      </div>
      <span style={{ fontWeight: 500, color: "var(--text)" }}>Could not load this</span>
      <span style={{ fontSize: 13, maxWidth: 360 }}>{message}</span>
      {onRetry && (
        <button type="button" className="btn btn-sm" onClick={onRetry}>
          Try again
        </button>
      )}
    </div>
  );
}

/** Bytes as a figure and a smaller unit: `1.24 TB`. */
export function Figure({ value, unit }: { value: string; unit?: string }) {
  return (
    <div className="kpi-value">
      {value}
      {unit && <small> {unit}</small>}
    </div>
  );
}
