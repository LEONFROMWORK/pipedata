import { NextResponse } from 'next/server'
import fs from 'fs'
import path from 'path'

const SETTINGS_FILE = path.join(process.cwd(), 'data', 'settings.json')

// Ensure data directory exists
function ensureDataDirectory() {
  const dataDir = path.dirname(SETTINGS_FILE)
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true })
  }
}

// Read settings from file
function readSettings() {
  ensureDataDirectory()
  
  try {
    if (fs.existsSync(SETTINGS_FILE)) {
      const data = fs.readFileSync(SETTINGS_FILE, 'utf8')
      return JSON.parse(data)
    }
  } catch (error) {
    console.error('Error reading settings:', error)
  }
  
  // Default settings
  return {
    model: 'anthropic/claude-3.5-sonnet',
    updated_at: new Date().toISOString()
  }
}

// Write settings to file
interface Settings {
  model: string;
  updated_at: string;
  [key: string]: unknown;
}

function writeSettings(settings: Settings) {
  ensureDataDirectory()
  
  try {
    settings.updated_at = new Date().toISOString()
    fs.writeFileSync(SETTINGS_FILE, JSON.stringify(settings, null, 2))
  } catch (error) {
    console.error('Error writing settings:', error)
    throw error
  }
}

export async function GET() {
  try {
    const settings = readSettings()
    return NextResponse.json(settings)
  } catch (error) {
    console.error('Error getting model settings:', error)
    return NextResponse.json({ 
      error: 'Failed to get model settings' 
    }, { status: 500 })
  }
}

export async function POST(request: Request) {
  try {
    const { model } = await request.json()
    
    if (!model) {
      return NextResponse.json({ 
        error: 'Model is required' 
      }, { status: 400 })
    }
    
    const settings = readSettings()
    settings.model = model
    
    writeSettings(settings)
    
    return NextResponse.json({ 
      success: true, 
      model: settings.model,
      updated_at: settings.updated_at 
    })
  } catch (error) {
    console.error('Error updating model settings:', error)
    return NextResponse.json({ 
      error: 'Failed to update model settings' 
    }, { status: 500 })
  }
}