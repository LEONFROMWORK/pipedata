"""
Excel Formula Validator Service
Validates Excel formulas and provides execution testing using ExcelJS
"""
import json
import logging
import re
import subprocess
import tempfile
import os
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from pathlib import Path
import asyncio
from dataclasses import dataclass
import uuid

logger = logging.getLogger('excel_validator_service')

@dataclass
class FormulaValidationResult:
    """Result of formula validation"""
    is_valid: bool
    formula: str
    error_message: Optional[str] = None
    suggestions: List[str] = None
    execution_result: Optional[Any] = None
    execution_time: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "formula": self.formula,
            "error_message": self.error_message,
            "suggestions": self.suggestions or [],
            "execution_result": self.execution_result,
            "execution_time": self.execution_time
        }

class ExcelValidatorService:
    """Excel formula validator using ExcelJS"""
    
    def __init__(self, node_path: str = "node"):
        self.node_path = node_path
        self.temp_dir = Path(tempfile.gettempdir()) / "excel_validator"
        self.temp_dir.mkdir(exist_ok=True)
        
        # Excel function registry
        self.excel_functions = self._load_excel_functions()
        
        # Validation statistics
        self.stats = {
            "total_validations": 0,
            "valid_formulas": 0,
            "invalid_formulas": 0,
            "execution_tests": 0,
            "execution_successes": 0,
            "last_validation": None
        }
        
        # Initialize ExcelJS validator
        self._create_validator_script()
    
    def _load_excel_functions(self) -> Dict[str, Dict[str, Any]]:
        """Load Excel functions registry"""
        return {
            # Math functions
            "SUM": {"category": "math", "min_args": 1, "max_args": 255},
            "AVERAGE": {"category": "math", "min_args": 1, "max_args": 255},
            "COUNT": {"category": "math", "min_args": 1, "max_args": 255},
            "MAX": {"category": "math", "min_args": 1, "max_args": 255},
            "MIN": {"category": "math", "min_args": 1, "max_args": 255},
            "ROUND": {"category": "math", "min_args": 1, "max_args": 2},
            "ABS": {"category": "math", "min_args": 1, "max_args": 1},
            
            # Logical functions
            "IF": {"category": "logical", "min_args": 2, "max_args": 3},
            "AND": {"category": "logical", "min_args": 1, "max_args": 255},
            "OR": {"category": "logical", "min_args": 1, "max_args": 255},
            "NOT": {"category": "logical", "min_args": 1, "max_args": 1},
            "IFERROR": {"category": "logical", "min_args": 2, "max_args": 2},
            "IFNA": {"category": "logical", "min_args": 2, "max_args": 2},
            
            # Lookup functions
            "VLOOKUP": {"category": "lookup", "min_args": 3, "max_args": 4},
            "HLOOKUP": {"category": "lookup", "min_args": 3, "max_args": 4},
            "XLOOKUP": {"category": "lookup", "min_args": 3, "max_args": 6},
            "INDEX": {"category": "lookup", "min_args": 2, "max_args": 4},
            "MATCH": {"category": "lookup", "min_args": 2, "max_args": 3},
            "CHOOSE": {"category": "lookup", "min_args": 2, "max_args": 255},
            
            # Text functions
            "LEFT": {"category": "text", "min_args": 1, "max_args": 2},
            "RIGHT": {"category": "text", "min_args": 1, "max_args": 2},
            "MID": {"category": "text", "min_args": 3, "max_args": 3},
            "LEN": {"category": "text", "min_args": 1, "max_args": 1},
            "CONCATENATE": {"category": "text", "min_args": 1, "max_args": 255},
            "TEXTJOIN": {"category": "text", "min_args": 3, "max_args": 255},
            
            # Date functions
            "TODAY": {"category": "date", "min_args": 0, "max_args": 0},
            "NOW": {"category": "date", "min_args": 0, "max_args": 0},
            "DATE": {"category": "date", "min_args": 3, "max_args": 3},
            "YEAR": {"category": "date", "min_args": 1, "max_args": 1},
            "MONTH": {"category": "date", "min_args": 1, "max_args": 1},
            "DAY": {"category": "date", "min_args": 1, "max_args": 1},
            
            # Statistical functions
            "SUMIF": {"category": "statistical", "min_args": 2, "max_args": 3},
            "SUMIFS": {"category": "statistical", "min_args": 3, "max_args": 255},
            "COUNTIF": {"category": "statistical", "min_args": 2, "max_args": 2},
            "COUNTIFS": {"category": "statistical", "min_args": 2, "max_args": 255},
            "AVERAGEIF": {"category": "statistical", "min_args": 2, "max_args": 3},
            "AVERAGEIFS": {"category": "statistical", "min_args": 3, "max_args": 255},
        }
    
    def _create_validator_script(self):
        """Create Node.js script for ExcelJS validation"""
        script_content = '''
const ExcelJS = require('exceljs');
const fs = require('fs');

async function validateFormula(formula, testData) {
    try {
        const workbook = new ExcelJS.Workbook();
        const worksheet = workbook.addWorksheet('Test');
        
        // Add test data if provided
        if (testData && testData.length > 0) {
            testData.forEach((row, rowIndex) => {
                row.forEach((value, colIndex) => {
                    worksheet.getCell(rowIndex + 1, colIndex + 1).value = value;
                });
            });
        }
        
        // Try to set the formula
        const testCell = worksheet.getCell('Z1');
        testCell.value = { formula: formula };
        
        // Calculate the workbook
        await workbook.xlsx.writeBuffer();
        
        // Get the calculated value
        const calculatedValue = testCell.value;
        
        return {
            success: true,
            formula: formula,
            result: calculatedValue,
            error: null
        };
        
    } catch (error) {
        return {
            success: false,
            formula: formula,
            result: null,
            error: error.message
        };
    }
}

async function main() {
    const args = process.argv.slice(2);
    if (args.length < 1) {
        console.error('Usage: node validator.js <formula> [test_data_file]');
        process.exit(1);
    }
    
    const formula = args[0];
    let testData = null;
    
    if (args.length > 1) {
        try {
            const testDataContent = fs.readFileSync(args[1], 'utf8');
            testData = JSON.parse(testDataContent);
        } catch (error) {
            console.error('Error reading test data:', error.message);
        }
    }
    
    const result = await validateFormula(formula, testData);
    console.log(JSON.stringify(result, null, 2));
}

main().catch(console.error);
'''
        
        script_path = self.temp_dir / "validator.js"
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        logger.info(f"Created ExcelJS validator script at {script_path}")
    
    def _extract_formulas(self, text: str) -> List[str]:
        """Extract Excel formulas from text"""
        # Pattern to match Excel formulas
        formula_pattern = r'=\s*[A-Z]+\s*\([^)]*\)'
        formulas = re.findall(formula_pattern, text, re.IGNORECASE)
        
        # Clean up formulas
        cleaned_formulas = []
        for formula in formulas:
            # Remove extra spaces
            cleaned = re.sub(r'\s+', '', formula)
            cleaned_formulas.append(cleaned)
        
        return cleaned_formulas
    
    def _validate_formula_syntax(self, formula: str) -> FormulaValidationResult:
        """Validate formula syntax"""
        if not formula.startswith('='):
            return FormulaValidationResult(
                is_valid=False,
                formula=formula,
                error_message="Formula must start with '='",
                suggestions=["Add '=' at the beginning of the formula"]
            )
        
        # Remove the leading '='
        formula_content = formula[1:]
        
        # Check for balanced parentheses
        if formula_content.count('(') != formula_content.count(')'):
            return FormulaValidationResult(
                is_valid=False,
                formula=formula,
                error_message="Unbalanced parentheses",
                suggestions=["Check that all parentheses are properly closed"]
            )
        
        # Extract function names
        function_pattern = r'([A-Z]+)\s*\('
        functions = re.findall(function_pattern, formula_content, re.IGNORECASE)
        
        # Check if functions exist
        unknown_functions = []
        for func in functions:
            if func.upper() not in self.excel_functions:
                unknown_functions.append(func)
        
        if unknown_functions:
            return FormulaValidationResult(
                is_valid=False,
                formula=formula,
                error_message=f"Unknown functions: {', '.join(unknown_functions)}",
                suggestions=[f"Check spelling of: {', '.join(unknown_functions)}"]
            )
        
        return FormulaValidationResult(
            is_valid=True,
            formula=formula
        )
    
    async def _execute_formula_test(self, formula: str, test_data: List[List[Any]] = None) -> Dict[str, Any]:
        """Execute formula using ExcelJS"""
        try:
            script_path = self.temp_dir / "validator.js"
            
            # Prepare test data file if provided
            test_data_file = None
            if test_data:
                test_data_file = self.temp_dir / f"test_data_{uuid.uuid4().hex}.json"
                with open(test_data_file, 'w') as f:
                    json.dump(test_data, f)
            
            # Prepare command
            cmd = [self.node_path, str(script_path), formula]
            if test_data_file:
                cmd.append(str(test_data_file))
            
            # Execute validation
            start_time = datetime.now()
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30  # 30 second timeout
            )
            end_time = datetime.now()
            
            execution_time = (end_time - start_time).total_seconds()
            
            # Clean up test data file
            if test_data_file and test_data_file.exists():
                test_data_file.unlink()
            
            if result.returncode == 0:
                output = json.loads(result.stdout)
                return {
                    "success": output["success"],
                    "result": output["result"],
                    "error": output["error"],
                    "execution_time": execution_time
                }
            else:
                return {
                    "success": False,
                    "result": None,
                    "error": result.stderr or "Unknown execution error",
                    "execution_time": execution_time
                }
            
        except subprocess.TimeoutExpired:
            return {
                "success": False,
                "result": None,
                "error": "Formula execution timed out",
                "execution_time": 30.0
            }
        except Exception as e:
            return {
                "success": False,
                "result": None,
                "error": str(e),
                "execution_time": 0.0
            }
    
    async def validate_formula(self, formula: str, test_data: List[List[Any]] = None) -> FormulaValidationResult:
        """Validate a single Excel formula"""
        try:
            self.stats["total_validations"] += 1
            
            # First, validate syntax
            syntax_result = self._validate_formula_syntax(formula)
            
            if not syntax_result.is_valid:
                self.stats["invalid_formulas"] += 1
                return syntax_result
            
            # Execute formula test
            self.stats["execution_tests"] += 1
            execution_result = await self._execute_formula_test(formula, test_data)
            
            if execution_result["success"]:
                self.stats["execution_successes"] += 1
                self.stats["valid_formulas"] += 1
                
                return FormulaValidationResult(
                    is_valid=True,
                    formula=formula,
                    execution_result=execution_result["result"],
                    execution_time=execution_result["execution_time"]
                )
            else:
                self.stats["invalid_formulas"] += 1
                
                return FormulaValidationResult(
                    is_valid=False,
                    formula=formula,
                    error_message=execution_result["error"],
                    suggestions=self._generate_suggestions(formula, execution_result["error"]),
                    execution_time=execution_result["execution_time"]
                )
            
        except Exception as e:
            logger.error(f"Error validating formula: {e}")
            self.stats["invalid_formulas"] += 1
            
            return FormulaValidationResult(
                is_valid=False,
                formula=formula,
                error_message=str(e),
                suggestions=["Check formula syntax and try again"]
            )
        finally:
            self.stats["last_validation"] = datetime.now().isoformat()
    
    def _generate_suggestions(self, formula: str, error: str) -> List[str]:
        """Generate suggestions based on error"""
        suggestions = []
        
        if "function" in error.lower():
            suggestions.append("Check function name spelling")
            suggestions.append("Verify function exists in your Excel version")
        
        if "reference" in error.lower():
            suggestions.append("Check cell references are valid")
            suggestions.append("Ensure referenced cells exist")
        
        if "argument" in error.lower():
            suggestions.append("Check number of function arguments")
            suggestions.append("Verify argument types are correct")
        
        if "syntax" in error.lower():
            suggestions.append("Check formula syntax")
            suggestions.append("Ensure parentheses are balanced")
        
        if not suggestions:
            suggestions.append("Check formula syntax and references")
        
        return suggestions
    
    async def validate_text_formulas(self, text: str) -> List[FormulaValidationResult]:
        """Validate all formulas found in text"""
        formulas = self._extract_formulas(text)
        
        if not formulas:
            return []
        
        results = []
        for formula in formulas:
            result = await self.validate_formula(formula)
            results.append(result)
        
        return results
    
    async def validate_ai_response(self, response: str) -> Dict[str, Any]:
        """Validate AI response containing Excel formulas"""
        try:
            # Extract formulas from response
            formulas = self._extract_formulas(response)
            
            if not formulas:
                return {
                    "has_formulas": False,
                    "validation_results": [],
                    "overall_valid": True,
                    "message": "No formulas found in response"
                }
            
            # Validate each formula
            validation_results = []
            valid_count = 0
            
            for formula in formulas:
                result = await self.validate_formula(formula)
                validation_results.append(result.to_dict())
                
                if result.is_valid:
                    valid_count += 1
            
            overall_valid = valid_count == len(formulas)
            
            return {
                "has_formulas": True,
                "formula_count": len(formulas),
                "valid_count": valid_count,
                "invalid_count": len(formulas) - valid_count,
                "validation_results": validation_results,
                "overall_valid": overall_valid,
                "validation_score": valid_count / len(formulas) if formulas else 0
            }
            
        except Exception as e:
            logger.error(f"Error validating AI response: {e}")
            return {
                "has_formulas": False,
                "validation_results": [],
                "overall_valid": False,
                "error": str(e)
            }
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get validation statistics"""
        return {
            "stats": self.stats.copy(),
            "excel_functions_count": len(self.excel_functions),
            "validator_ready": (self.temp_dir / "validator.js").exists(),
            "timestamp": datetime.now().isoformat()
        }
    
    def cleanup(self):
        """Clean up temporary files"""
        try:
            if self.temp_dir.exists():
                for file in self.temp_dir.glob("*"):
                    if file.is_file():
                        file.unlink()
        except Exception as e:
            logger.error(f"Error cleaning up temp files: {e}")

# Singleton instance
_excel_validator_service = None

async def get_excel_validator_service() -> ExcelValidatorService:
    """Get singleton Excel validator service instance"""
    global _excel_validator_service
    if _excel_validator_service is None:
        _excel_validator_service = ExcelValidatorService()
    return _excel_validator_service

def cleanup_excel_validator_service():
    """Clean up Excel validator service"""
    global _excel_validator_service
    if _excel_validator_service:
        _excel_validator_service.cleanup()
        _excel_validator_service = None