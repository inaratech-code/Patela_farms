import type { Metadata } from "next";
import { Suspense } from "react";
import { LoginClient } from "@/app/login/LoginClient";

export const metadata: Metadata = {
  title: "Login",
  description: "Sign in to Patela Farm Management",
};

export default function LoginPage() {
  // useSearchParams is used inside LoginClient; wrap in Suspense for Next build.
  return (
    <Suspense fallback={<div className="min-h-screen bg-background" />}>
      <LoginClient />
    </Suspense>
  );
}

