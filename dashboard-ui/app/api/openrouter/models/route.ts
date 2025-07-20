import { NextResponse } from 'next/server'

export async function GET() {
  try {
    const apiKey = process.env.OPENROUTER_API_KEY;
    
    if (!apiKey) {
      // Return mock models if no API key
      const mockModels = [
        {
          id: 'anthropic/claude-3.5-sonnet',
          name: 'Claude 3.5 Sonnet',
          category: 'Anthropic',
          pricing: { prompt: '0.000003', completion: '0.000015' },
          context_length: 200000,
          is_popular: true,
          description: 'Most intelligent model, ideal for complex analysis'
        },
        {
          id: 'anthropic/claude-3-haiku',
          name: 'Claude 3 Haiku',
          category: 'Anthropic',
          pricing: { prompt: '0.00000025', completion: '0.00000125' },
          context_length: 200000,
          is_popular: true,
          description: 'Fastest model for simple tasks'
        },
        {
          id: 'openai/gpt-4o',
          name: 'GPT-4o',
          category: 'OpenAI',
          pricing: { prompt: '0.0000025', completion: '0.00001' },
          context_length: 128000,
          is_popular: true,
          description: 'Latest OpenAI model with vision'
        },
        {
          id: 'openai/gpt-4o-mini',
          name: 'GPT-4o Mini',
          category: 'OpenAI',
          pricing: { prompt: '0.00000015', completion: '0.0000006' },
          context_length: 128000,
          is_popular: true,
          description: 'Affordable and fast OpenAI model'
        },
        {
          id: 'google/gemini-pro',
          name: 'Gemini Pro',
          category: 'Google',
          pricing: { prompt: '0.000001', completion: '0.000002' },
          context_length: 32768,
          is_popular: false,
          description: 'Google\'s multimodal model'
        }
      ]
      return NextResponse.json(mockModels)
    }

    try {
      const response = await fetch('https://openrouter.ai/api/v1/models', {
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        }
      })

      if (!response.ok) {
        throw new Error(`OpenRouter API error: ${response.status} ${response.statusText}`)
      }

      const data = await response.json()
      const models = data.data || []
      
      // Popular models for Excel QA tasks
      const popularModels = [
        'anthropic/claude-3.5-sonnet',
        'anthropic/claude-3-haiku',
        'openai/gpt-4o',
        'openai/gpt-4o-mini',
        'openai/gpt-3.5-turbo',
        'meta-llama/llama-3.1-70b-instruct',
        'google/gemini-pro',
        'cohere/command-r-plus'
      ]
      
      // Format models for dashboard
      interface ModelResponse {
        id: string;
        name?: string;
        pricing?: {
          prompt?: string;
          completion?: string;
        };
        context_length?: number;
        description?: string;
      }

      const formattedModels = models.map((model: ModelResponse) => {
        const modelId = model.id || ''
        
        // Determine category
        let category = 'Other'
        if (modelId.includes('anthropic')) category = 'Anthropic'
        else if (modelId.includes('openai')) category = 'OpenAI'
        else if (modelId.includes('google')) category = 'Google'
        else if (modelId.includes('meta')) category = 'Meta'
        else if (modelId.includes('cohere')) category = 'Cohere'
        else if (modelId.includes('mistral')) category = 'Mistral'
        
        return {
          id: modelId,
          name: model.name || modelId,
          category: category,
          pricing: {
            prompt: model.pricing?.prompt || '0',
            completion: model.pricing?.completion || '0'
          },
          context_length: model.context_length || 0,
          is_popular: popularModels.includes(modelId),
          description: model.description || ''
        }
      })
      
      // Sort by popularity and category
      interface FormattedModel {
        id: string;
        name: string;
        category: string;
        pricing: {
          prompt: string;
          completion: string;
        };
        context_length: number;
        is_popular: boolean;
        description: string;
      }

      formattedModels.sort((a: FormattedModel, b: FormattedModel) => {
        if (a.is_popular && !b.is_popular) return -1
        if (!a.is_popular && b.is_popular) return 1
        if (a.category !== b.category) return a.category.localeCompare(b.category)
        return a.name.localeCompare(b.name)
      })
      
      return NextResponse.json(formattedModels)
    } catch (fetchError) {
      console.error('Error fetching from OpenRouter:', fetchError)
      // Return mock models as fallback
      const mockModels = [
        {
          id: 'anthropic/claude-3.5-sonnet',
          name: 'Claude 3.5 Sonnet',
          category: 'Anthropic',
          pricing: { prompt: '0.000003', completion: '0.000015' },
          context_length: 200000,
          is_popular: true,
          description: 'Most intelligent model, ideal for complex analysis'
        },
        {
          id: 'anthropic/claude-3-haiku',
          name: 'Claude 3 Haiku',
          category: 'Anthropic',
          pricing: { prompt: '0.00000025', completion: '0.00000125' },
          context_length: 200000,
          is_popular: true,
          description: 'Fastest model for simple tasks'
        }
      ]
      return NextResponse.json(mockModels)
    }
  } catch (error) {
    console.error('Error fetching models:', error)
    return NextResponse.json({ 
      error: 'Failed to fetch models' 
    }, { status: 500 })
  }
}