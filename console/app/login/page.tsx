import type { Metadata } from "next";
import { Suspense } from "react";

import { SignIn } from "@/components/SignIn";

export const metadata: Metadata = { title: "Sign in" };

export default function LoginPage() {
  return (
    <Suspense>
      <SignIn />
    </Suspense>
  );
}
