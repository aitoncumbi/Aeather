import Link from "next/link";

export default function NotFound() {
  return (
    <main className="signin">
      <div className="signin-box" style={{ alignItems: "center", textAlign: "center", gap: 12 }}>
        <h1 style={{ fontSize: 24, fontWeight: 600, letterSpacing: "-0.02em" }}>Page not found</h1>
        <p className="muted">This console has no page at this address.</p>
        <Link href="/" className="btn btn-sm">
          Go to the overview
        </Link>
      </div>
    </main>
  );
}
