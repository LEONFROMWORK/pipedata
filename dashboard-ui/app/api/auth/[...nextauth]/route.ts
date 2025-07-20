import NextAuth from "next-auth"
import GoogleProvider from "next-auth/providers/google"
import { NextAuthOptions } from "next-auth"

const authOptions: NextAuthOptions = {
  providers: [
    GoogleProvider({
      clientId: process.env.GOOGLE_CLIENT_ID!,
      clientSecret: process.env.GOOGLE_CLIENT_SECRET!,
    }),
  ],
  callbacks: {
    async signIn({ user }) {
      // 허용된 관리자 이메일 목록 확인
      const adminEmails = process.env.ADMIN_EMAILS?.split(',').map(email => email.trim()) || [];
      
      if (!user.email || !adminEmails.includes(user.email)) {
        console.log(`Access denied for email: ${user.email}`);
        return false;
      }
      
      console.log(`Access granted for admin: ${user.email}`);
      return true;
    },
    async session({ session, token }) {
      // 세션에 사용자 정보 포함
      if (session.user) {
        session.user.id = token.sub!;
      }
      return session;
    },
    async jwt({ token, user }) {
      if (user) {
        token.id = user.id;
      }
      return token;
    },
  },
  pages: {
    signIn: '/login',
    error: '/login',
  },
  session: {
    strategy: 'jwt',
  },
  secret: process.env.NEXTAUTH_SECRET,
};

const handler = NextAuth(authOptions);

export { handler as GET, handler as POST };