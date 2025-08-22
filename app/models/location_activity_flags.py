from sqlalchemy import Column, String, Integer, ForeignKey, Boolean, UniqueConstraint
from sqlalchemy.orm import relationship
from app.core.database import Base


class LocationActivityFlags(Base):
    """
    Captures all the Y/N flags for regulated activities and services for each location per period.
    This corresponds to the many Y/N fields at the end of the CQC data records.
    """
    __tablename__ = "location_activity_flags"

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(String(20), ForeignKey("locations.location_id"), nullable=False)
    period_id = Column(Integer, ForeignKey("data_periods.period_id"), nullable=False)
    
    # Regulated Activities (Y/N flags)
    accommodation_nursing_personal_care = Column(Boolean, default=False)
    treatment_disease_disorder_injury = Column(Boolean, default=False) 
    assessment_medical_treatment = Column(Boolean, default=False)
    surgical_procedures = Column(Boolean, default=False)
    diagnostic_screening = Column(Boolean, default=False)
    management_supply_blood = Column(Boolean, default=False)
    transport_services = Column(Boolean, default=False)
    maternity_midwifery = Column(Boolean, default=False)
    termination_pregnancies = Column(Boolean, default=False)
    services_slimming = Column(Boolean, default=False)
    nursing_care = Column(Boolean, default=False)
    personal_care = Column(Boolean, default=False)
    accommodation_persons_detoxification = Column(Boolean, default=False)
    accommodation_persons_past_present_alcohol_dependence = Column(Boolean, default=False)
    
    # Service Types (Y/N flags)
    acute_services_substance_misuse = Column(Boolean, default=False)
    acute_services_mental_health = Column(Boolean, default=False)
    ambulance_service = Column(Boolean, default=False)
    blood_bone_marrow_stem_cell_donation = Column(Boolean, default=False)
    care_home_nursing = Column(Boolean, default=False)
    care_home_without_nursing = Column(Boolean, default=False)
    community_adult_learning_disability = Column(Boolean, default=False)
    community_health_care = Column(Boolean, default=False)
    community_mental_health = Column(Boolean, default=False)
    community_substance_misuse = Column(Boolean, default=False)
    dental_service = Column(Boolean, default=False)
    diagnostic_imaging = Column(Boolean, default=False)
    doctors_consultation = Column(Boolean, default=False)
    doctors_treatment = Column(Boolean, default=False)
    domiciliary_care = Column(Boolean, default=False)
    extra_corporeal_membrane_oxygenation = Column(Boolean, default=False)
    hospice_services = Column(Boolean, default=False)
    hospital_services_acute = Column(Boolean, default=False)
    hospital_services_long_term = Column(Boolean, default=False)
    hyperbaric_chamber = Column(Boolean, default=False)
    independent_clinic = Column(Boolean, default=False)
    independent_medical_agency = Column(Boolean, default=False)
    long_term_conditions = Column(Boolean, default=False)
    mobile_doctors = Column(Boolean, default=False)
    occupational_health = Column(Boolean, default=False)
    prison_healthcare = Column(Boolean, default=False)
    rehabilitation_services = Column(Boolean, default=False)
    renal_dialysis = Column(Boolean, default=False)
    residential_substance_misuse = Column(Boolean, default=False)
    shared_lives = Column(Boolean, default=False)
    specialist_college = Column(Boolean, default=False)
    supported_living = Column(Boolean, default=False)
    urgent_care = Column(Boolean, default=False)
    
    # Service User Bands (Y/N flags)  
    children_0_3_years = Column(Boolean, default=False)
    children_4_12_years = Column(Boolean, default=False)
    children_13_18_years = Column(Boolean, default=False)
    adults_18_65_years = Column(Boolean, default=False)
    older_people_65_plus = Column(Boolean, default=False)
    dementia = Column(Boolean, default=False)
    learning_disabilities_autistic = Column(Boolean, default=False)
    mental_health_needs = Column(Boolean, default=False)
    physical_disability = Column(Boolean, default=False)
    sensory_impairment = Column(Boolean, default=False)
    substance_misuse = Column(Boolean, default=False)
    eating_disorders = Column(Boolean, default=False)
    mothers_babies = Column(Boolean, default=False)
    
    # Ensure unique location per period
    __table_args__ = (
        UniqueConstraint('location_id', 'period_id', name='uq_location_period_flags'),
    )

    # Relationships  
    location = relationship("Location")
    data_period = relationship("DataPeriod")