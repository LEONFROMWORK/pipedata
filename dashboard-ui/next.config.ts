import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: 'standalone',
  // 이미지 최적화 설정
  images: {
    unoptimized: true,
  },
  // 빌드 시 타입 체크 활성화
  typescript: {
    ignoreBuildErrors: false,
  },
  eslint: {
    ignoreDuringBuilds: false,
  },
};

export default nextConfig;
