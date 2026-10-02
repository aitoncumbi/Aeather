import type { Metadata } from "next";
import { Suspense } from "react";

import { Keys } from "@/components/Keys";

export const metadata: Metadata = { title: "Access keys" };

export default function KeysPage() {
  return (
    <Suspense>
      <Keys />
    </Suspense>
  );
}
