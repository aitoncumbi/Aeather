import type { Metadata } from "next";
import { Suspense } from "react";

import { Objects } from "@/components/Objects";

export const metadata: Metadata = { title: "Object browser" };

export default function ObjectsPage() {
  return (
    <Suspense>
      <Objects />
    </Suspense>
  );
}
