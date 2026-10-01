import type { Metadata } from "next";
import { Suspense } from "react";

import { Buckets } from "@/components/Buckets";

export const metadata: Metadata = { title: "Buckets" };

export default function BucketsPage() {
  return (
    <Suspense>
      <Buckets />
    </Suspense>
  );
}
