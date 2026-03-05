"""
Variable Compensation Calculator - Healthcare Sales Platform
Processing compensation for 500+ sales team members with complex rule engine
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging
from decimal import Decimal, ROUND_HALF_UP

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CompensationType(Enum):
    """Types of compensation components"""
    BASE_COMMISSION = "base_commission"
    VOLUME_BONUS = "volume_bonus"
    RETENTION_BONUS = "retention_bonus"
    QUALITY_BONUS = "quality_bonus"
    TEAM_BONUS = "team_bonus"
    SPECIAL_INCENTIVE = "special_incentive"


@dataclass
class CompensationRule:
    """Rule definition for compensation calculation"""
    rule_id: str
    rule_type: CompensationType
    description: str
    base_rate: float
    tiers: List[Dict[str, Any]]
    conditions: Dict[str, Any]
    priority: int
    active: bool = True
    effective_date: datetime = field(default_factory=datetime.now)


@dataclass
class SalesMetrics:
    """Sales performance metrics for a period"""
    seller_id: str
    period: str
    new_contracts: int
    renewed_contracts: int
    total_beneficiaries: int
    contract_value: float
    retention_rate: float
    quality_score: float
    team_performance: float
    special_achievements: List[str]


class VariableCompensationEngine:
    """
    Complex variable compensation calculator
    Handles 7-step workflow from data collection to payment processing
    """

    def __init__(self):
        """Initialize compensation engine with business rules"""
        self.rules = self._initialize_rules()
        self.audit_trail = []

    def _initialize_rules(self) -> List[CompensationRule]:
        """
        Initialize compensation rules based on business requirements
        These rules were inspired by compensation patterns from large-scale healthcare operations
        """
        rules = []

        # Base commission rule
        rules.append(CompensationRule(
            rule_id="BASE_001",
            rule_type=CompensationType.BASE_COMMISSION,
            description="Base commission on new contracts",
            base_rate=0.02,  # 2% base
            tiers=[
                {"min": 0, "max": 10, "rate": 0.02},
                {"min": 11, "max": 50, "rate": 0.025},
                {"min": 51, "max": 100, "rate": 0.03},
                {"min": 101, "max": float('inf'), "rate": 0.035}
            ],
            conditions={"contract_type": "new"},
            priority=1
        ))

        # Volume bonus rule
        rules.append(CompensationRule(
            rule_id="VOL_001",
            rule_type=CompensationType.VOLUME_BONUS,
            description="Volume bonus for high performers",
            base_rate=0.0,
            tiers=[
                {"min": 100000, "max": 500000, "bonus": 1000},
                {"min": 500001, "max": 1000000, "bonus": 5000},
                {"min": 1000001, "max": float('inf'), "bonus": 10000}
            ],
            conditions={"min_contracts": 10},
            priority=2
        ))

        # Retention bonus rule
        rules.append(CompensationRule(
            rule_id="RET_001",
            rule_type=CompensationType.RETENTION_BONUS,
            description="Bonus for high retention rates",
            base_rate=0.01,
            tiers=[
                {"min": 0.8, "max": 0.9, "multiplier": 1.0},
                {"min": 0.9, "max": 0.95, "multiplier": 1.5},
                {"min": 0.95, "max": 1.0, "multiplier": 2.0}
            ],
            conditions={"min_retention": 0.8},
            priority=3
        ))

        # Quality bonus rule
        rules.append(CompensationRule(
            rule_id="QUAL_001",
            rule_type=CompensationType.QUALITY_BONUS,
            description="Quality bonus for low complaints",
            base_rate=500,
            tiers=[
                {"quality_score": 4.0, "bonus": 500},
                {"quality_score": 4.5, "bonus": 1000},
                {"quality_score": 4.8, "bonus": 2000}
            ],
            conditions={"min_quality": 4.0},
            priority=4
        ))

        return rules

    # ============= MAIN CALCULATION PIPELINE =============

    def calculate_compensation(self,
                              metrics: SalesMetrics,
                              payment_period: str) -> Dict[str, Any]:
        """
        Main compensation calculation pipeline
        Implements the 7-step workflow from the production system

        Steps:
        1. Data collection
        2. Validation
        3. Rule application
        4. Calculation
        5. Approval workflow
        6. Final validation
        7. Payment processing
        """
        logger.info(f"Starting compensation calculation for {metrics.seller_id} - Period: {payment_period}")

        # Step 1: Data Collection (already in metrics)
        compensation_details = {
            'seller_id': metrics.seller_id,
            'period': payment_period,
            'calculation_date': datetime.now(),
            'components': {},
            'total': Decimal('0'),
            'status': 'CALCULATING'
        }

        # Step 2: Validation
        validation_result = self._validate_metrics(metrics)
        if not validation_result['valid']:
            compensation_details['status'] = 'VALIDATION_FAILED'
            compensation_details['errors'] = validation_result['errors']
            return compensation_details

        # Step 3 & 4: Apply Rules and Calculate
        for rule in sorted(self.rules, key=lambda r: r.priority):
            if self._check_rule_eligibility(metrics, rule):
                component_value = self._calculate_component(metrics, rule)
                compensation_details['components'][rule.rule_type.value] = {
                    'rule_id': rule.rule_id,
                    'description': rule.description,
                    'value': float(component_value),
                    'calculation_details': self._get_calculation_details(metrics, rule)
                }

        # Calculate total compensation
        total = sum(Decimal(str(comp['value']))
                   for comp in compensation_details['components'].values())
        compensation_details['total'] = float(total)

        # Step 5: Approval Workflow Simulation
        compensation_details['approval'] = self._simulate_approval_workflow(
            compensation_details['total'],
            metrics.seller_id
        )

        # Step 6: Final Validation
        final_validation = self._final_validation(compensation_details)
        compensation_details['final_validation'] = final_validation

        # Step 7: Payment Processing Preparation
        if final_validation['approved']:
            compensation_details['status'] = 'APPROVED'
            compensation_details['payment_info'] = self._prepare_payment(compensation_details)
        else:
            compensation_details['status'] = 'REJECTED'
            compensation_details['rejection_reason'] = final_validation['reason']

        # Audit trail
        self._log_audit_trail(compensation_details)

        return compensation_details

    # ============= VALIDATION FUNCTIONS =============

    def _validate_metrics(self, metrics: SalesMetrics) -> Dict[str, Any]:
        """
        Validate sales metrics before calculation
        Critical for ensuring accurate compensation
        """
        errors = []

        # Check for negative values
        if metrics.new_contracts < 0:
            errors.append("Negative new contracts count")
        if metrics.contract_value < 0:
            errors.append("Negative contract value")

        # Check retention rate bounds
        if not 0 <= metrics.retention_rate <= 1:
            errors.append(f"Invalid retention rate: {metrics.retention_rate}")

        # Check quality score bounds
        if not 0 <= metrics.quality_score <= 5:
            errors.append(f"Invalid quality score: {metrics.quality_score}")

        # Business rule: minimum activity threshold
        if metrics.new_contracts == 0 and metrics.renewed_contracts == 0:
            errors.append("No activity in period")

        return {
            'valid': len(errors) == 0,
            'errors': errors
        }

    def _final_validation(self, compensation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Final validation before payment approval
        Includes fraud detection and limit checks
        """
        total = compensation['total']

        # Check maximum compensation limits
        MAX_COMPENSATION = 50000  # Business rule
        if total > MAX_COMPENSATION:
            return {
                'approved': False,
                'reason': f'Exceeds maximum compensation limit of {MAX_COMPENSATION}'
            }

        # Check minimum threshold
        MIN_COMPENSATION = 100  # Don't process tiny payments
        if total < MIN_COMPENSATION:
            return {
                'approved': False,
                'reason': f'Below minimum compensation threshold of {MIN_COMPENSATION}'
            }

        # Check for suspicious patterns
        components = compensation['components']
        if len(components) == 0:
            return {
                'approved': False,
                'reason': 'No compensation components calculated'
            }

        return {
            'approved': True,
            'reason': 'All validations passed'
        }

    # ============= CALCULATION FUNCTIONS =============

    def _check_rule_eligibility(self, metrics: SalesMetrics, rule: CompensationRule) -> bool:
        """Check if seller is eligible for a specific compensation rule"""
        if not rule.active:
            return False

        conditions = rule.conditions

        # Check contract type conditions
        if 'contract_type' in conditions:
            if conditions['contract_type'] == 'new' and metrics.new_contracts == 0:
                return False

        # Check minimum contracts
        if 'min_contracts' in conditions:
            total_contracts = metrics.new_contracts + metrics.renewed_contracts
            if total_contracts < conditions['min_contracts']:
                return False

        # Check retention threshold
        if 'min_retention' in conditions:
            if metrics.retention_rate < conditions['min_retention']:
                return False

        # Check quality threshold
        if 'min_quality' in conditions:
            if metrics.quality_score < conditions['min_quality']:
                return False

        return True

    def _calculate_component(self, metrics: SalesMetrics, rule: CompensationRule) -> Decimal:
        """
        Calculate compensation component based on rule
        Uses Decimal for precise financial calculations
        """
        value = Decimal('0')

        if rule.rule_type == CompensationType.BASE_COMMISSION:
            # Tiered commission based on number of contracts
            rate = self._get_tier_rate(metrics.new_contracts, rule.tiers, 'rate')
            value = Decimal(str(metrics.contract_value)) * Decimal(str(rate))

        elif rule.rule_type == CompensationType.VOLUME_BONUS:
            # Fixed bonus based on total volume
            for tier in rule.tiers:
                if tier['min'] <= metrics.contract_value <= tier['max']:
                    value = Decimal(str(tier['bonus']))
                    break

        elif rule.rule_type == CompensationType.RETENTION_BONUS:
            # Bonus multiplied by retention performance
            multiplier = self._get_tier_rate(metrics.retention_rate, rule.tiers, 'multiplier')
            base_bonus = Decimal(str(metrics.contract_value)) * Decimal(str(rule.base_rate))
            value = base_bonus * Decimal(str(multiplier))

        elif rule.rule_type == CompensationType.QUALITY_BONUS:
            # Fixed bonus for quality scores
            for tier in rule.tiers:
                if metrics.quality_score >= tier['quality_score']:
                    value = Decimal(str(tier['bonus']))

        elif rule.rule_type == CompensationType.TEAM_BONUS:
            # Team performance bonus
            if metrics.team_performance > 1.0:  # Above average
                value = Decimal(str(rule.base_rate)) * Decimal(str(metrics.team_performance))

        # Round to 2 decimal places for currency
        value = value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        return value

    def _get_tier_rate(self, metric_value: float, tiers: List[Dict], rate_key: str) -> float:
        """Get rate from tiered structure"""
        for tier in tiers:
            if 'min' in tier and 'max' in tier:
                if tier['min'] <= metric_value <= tier['max']:
                    return tier[rate_key]
        return 0.0

    def _get_calculation_details(self, metrics: SalesMetrics, rule: CompensationRule) -> Dict[str, Any]:
        """Get detailed calculation breakdown for transparency"""
        details = {
            'rule_type': rule.rule_type.value,
            'base_rate': rule.base_rate,
            'metrics_used': {}
        }

        if rule.rule_type == CompensationType.BASE_COMMISSION:
            details['metrics_used'] = {
                'new_contracts': metrics.new_contracts,
                'contract_value': metrics.contract_value
            }
        elif rule.rule_type == CompensationType.RETENTION_BONUS:
            details['metrics_used'] = {
                'retention_rate': metrics.retention_rate,
                'contract_value': metrics.contract_value
            }

        return details

    # ============= WORKFLOW FUNCTIONS =============

    def _simulate_approval_workflow(self, total: float, seller_id: str) -> Dict[str, Any]:
        """
        Simulate approval workflow based on amount
        In production, this integrates with workflow engine
        """
        approval_levels = []

        if total < 5000:
            approval_levels.append({
                'level': 'AUTOMATIC',
                'approver': 'SYSTEM',
                'timestamp': datetime.now().isoformat(),
                'status': 'APPROVED'
            })
        elif total < 20000:
            approval_levels.append({
                'level': 'MANAGER',
                'approver': f'MANAGER_{seller_id[:3]}',
                'timestamp': datetime.now().isoformat(),
                'status': 'APPROVED'
            })
        else:
            approval_levels.append({
                'level': 'DIRECTOR',
                'approver': 'SALES_DIRECTOR',
                'timestamp': datetime.now().isoformat(),
                'status': 'PENDING'
            })

        return {
            'required_approvals': len(approval_levels),
            'approvals': approval_levels,
            'final_status': approval_levels[-1]['status']
        }

    def _prepare_payment(self, compensation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare payment information for processing
        Integrates with payment systems in production
        """
        return {
            'payment_id': f"PAY_{compensation['seller_id']}_{compensation['period']}_{int(datetime.now().timestamp())}",
            'amount': compensation['total'],
            'currency': 'BRL',
            'payment_date': (datetime.now() + timedelta(days=5)).isoformat(),
            'payment_method': 'BANK_TRANSFER',
            'status': 'SCHEDULED'
        }

    def _log_audit_trail(self, compensation: Dict[str, Any]):
        """
        Log calculation for audit purposes
        Critical for compliance and dispute resolution
        """
        audit_entry = {
            'timestamp': datetime.now().isoformat(),
            'seller_id': compensation['seller_id'],
            'period': compensation['period'],
            'total': compensation['total'],
            'status': compensation['status'],
            'components': list(compensation['components'].keys())
        }
        self.audit_trail.append(audit_entry)
        logger.info(f"Audit trail logged: {audit_entry}")

    # ============= REPORTING FUNCTIONS =============

    def generate_compensation_report(self, period: str) -> pd.DataFrame:
        """
        Generate compensation report for all sellers in a period
        Used by finance team for payment processing
        """
        report_data = []

        # In production, this would query from database
        # Here we simulate with sample data
        for seller_id in [f"SELLER_{i:03d}" for i in range(1, 11)]:
            metrics = self._generate_sample_metrics(seller_id, period)
            compensation = self.calculate_compensation(metrics, period)

            report_data.append({
                'seller_id': seller_id,
                'period': period,
                'new_contracts': metrics.new_contracts,
                'contract_value': metrics.contract_value,
                'retention_rate': metrics.retention_rate,
                'quality_score': metrics.quality_score,
                'base_commission': compensation['components'].get('base_commission', {}).get('value', 0),
                'volume_bonus': compensation['components'].get('volume_bonus', {}).get('value', 0),
                'retention_bonus': compensation['components'].get('retention_bonus', {}).get('value', 0),
                'quality_bonus': compensation['components'].get('quality_bonus', {}).get('value', 0),
                'total_compensation': compensation['total'],
                'status': compensation['status']
            })

        return pd.DataFrame(report_data)

    def _generate_sample_metrics(self, seller_id: str, period: str) -> SalesMetrics:
        """Generate sample metrics for demonstration"""
        np.random.seed(hash(seller_id) % 1000)  # Consistent random for each seller

        return SalesMetrics(
            seller_id=seller_id,
            period=period,
            new_contracts=np.random.randint(5, 150),
            renewed_contracts=np.random.randint(10, 200),
            total_beneficiaries=np.random.randint(100, 5000),
            contract_value=np.random.uniform(50000, 1500000),
            retention_rate=np.random.uniform(0.75, 0.98),
            quality_score=np.random.uniform(3.5, 5.0),
            team_performance=np.random.uniform(0.8, 1.3),
            special_achievements=[]
        )


# ============= DEMONSTRATION =============

def demonstrate_compensation_system():
    """
    Demonstrate the variable compensation system
    Shows the complexity handled for 500+ sales representatives in a major healthcare organization
    """
    engine = VariableCompensationEngine()

    print("=" * 70)
    print("VARIABLE COMPENSATION SYSTEM DEMONSTRATION")
    print("Processing compensation for healthcare sales team")
    print("=" * 70)

    # Example 1: High performer
    print("\n📊 EXAMPLE 1: High Performer")
    print("-" * 70)

    high_performer = SalesMetrics(
        seller_id="SELLER_001",
        period="2024-01",
        new_contracts=120,
        renewed_contracts=200,
        total_beneficiaries=4500,
        contract_value=1200000,
        retention_rate=0.96,
        quality_score=4.8,
        team_performance=1.2,
        special_achievements=["TOP_SELLER_Q1"]
    )

    compensation = engine.calculate_compensation(high_performer, "2024-01")

    print(f"Seller ID: {compensation['seller_id']}")
    print(f"Period: {compensation['period']}")
    print("\nCompensation Components:")
    for component, details in compensation['components'].items():
        print(f"  {component}: R$ {details['value']:,.2f}")
    print(f"\nTOTAL COMPENSATION: R$ {compensation['total']:,.2f}")
    print(f"Status: {compensation['status']}")

    # Example 2: Average performer
    print("\n📊 EXAMPLE 2: Average Performer")
    print("-" * 70)

    avg_performer = SalesMetrics(
        seller_id="SELLER_002",
        period="2024-01",
        new_contracts=25,
        renewed_contracts=50,
        total_beneficiaries=800,
        contract_value=250000,
        retention_rate=0.85,
        quality_score=4.2,
        team_performance=1.0,
        special_achievements=[]
    )

    compensation = engine.calculate_compensation(avg_performer, "2024-01")

    print(f"Seller ID: {compensation['seller_id']}")
    print(f"Period: {compensation['period']}")
    print("\nCompensation Components:")
    for component, details in compensation['components'].items():
        print(f"  {component}: R$ {details['value']:,.2f}")
    print(f"\nTOTAL COMPENSATION: R$ {compensation['total']:,.2f}")
    print(f"Status: {compensation['status']}")

    # Generate full report
    print("\n📈 COMPENSATION REPORT SUMMARY")
    print("-" * 70)

    report = engine.generate_compensation_report("2024-01")

    print("\nTop 5 Earners:")
    top_earners = report.nlargest(5, 'total_compensation')[
        ['seller_id', 'new_contracts', 'contract_value', 'total_compensation']
    ]
    for _, row in top_earners.iterrows():
        print(f"  {row['seller_id']}: R$ {row['total_compensation']:,.2f} "
              f"({row['new_contracts']} contracts, R$ {row['contract_value']:,.2f} value)")

    print(f"\nTotal Compensation Pool: R$ {report['total_compensation'].sum():,.2f}")
    print(f"Average Compensation: R$ {report['total_compensation'].mean():,.2f}")
    print(f"Approved Payments: {len(report[report['status'] == 'APPROVED'])}/{len(report)}")

    print("\n" + "=" * 70)
    print("💡 KEY FEATURES DEMONSTRATED:")
    print("=" * 70)
    print("✅ Multi-tier commission structure")
    print("✅ Complex rule engine with priorities")
    print("✅ 7-step workflow implementation")
    print("✅ Precise financial calculations with Decimal")
    print("✅ Comprehensive audit trail")
    print("✅ Approval workflow simulation")
    print("\n🎯 Production Scale: 500+ sellers, monthly processing")


if __name__ == "__main__":
    demonstrate_compensation_system()