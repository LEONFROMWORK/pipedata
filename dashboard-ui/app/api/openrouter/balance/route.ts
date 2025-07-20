import { NextResponse } from 'next/server'

export async function GET() {
  try {
    const apiKey = process.env.OPENROUTER_API_KEY;
    
    if (!apiKey) {
      return NextResponse.json(
        { error: 'OpenRouter API key not configured' },
        { status: 500 }
      );
    }

    console.log('Fetching real-time balance from OpenRouter API...');

    // Use the correct OpenRouter credits endpoint
    const creditsResponse = await fetch('https://openrouter.ai/api/v1/credits', {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      },
    });

    if (!creditsResponse.ok) {
      console.error(`OpenRouter credits API error: ${creditsResponse.status}`);
      const errorText = await creditsResponse.text();
      console.error('Error response:', errorText);
      
      return NextResponse.json({
        error: `Failed to fetch credits from OpenRouter API: ${creditsResponse.status}`
      }, { status: creditsResponse.status });
    }

    const creditsData = await creditsResponse.json();
    console.log('Real credits data:', creditsData);
    
    // Get additional key information
    let keyData = null;
    try {
      const keyResponse = await fetch('https://openrouter.ai/api/v1/api-keys/current', {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
      });

      if (keyResponse.ok) {
        keyData = await keyResponse.json();
        console.log('Key data:', keyData);
      }
    } catch {
      console.log('Key endpoint failed, continuing with credits data only');
    }

    // Extract balance with highest precision
    const totalCredits = creditsData.data?.total_credits || creditsData.balance || creditsData.credits || 0;
    const totalUsage = creditsData.data?.total_usage || creditsData.usage || 0;
    const remainingBalance = totalCredits - totalUsage;
    
    // Return real data from OpenRouter API
    const balanceInfo = {
      balance: remainingBalance,
      usage: totalUsage,
      limit: creditsData.data?.limit || keyData?.limit || 25.00,
      is_free_tier: creditsData.data?.is_free_tier || keyData?.is_free_tier || false,
      rate_limit: creditsData.data?.rate_limit || keyData?.rate_limit || {},
      last_updated: new Date().toISOString()
    };

    return NextResponse.json(balanceInfo);
  } catch (error) {
    console.error('Error fetching balance:', error)
    return NextResponse.json({ 
      error: 'Failed to fetch balance information' 
    }, { status: 500 })
  }
}