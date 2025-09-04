from pydantic import BaseModel
from typing import Optional


class LocationActivityFlagsBase(BaseModel):
    location_id: int
    period_id: int
    
    # Regulated Activities (Y/N flags)
    accommodation_nursing_personal_care: bool = False
    treatment_disease_disorder_injury: bool = False
    assessment_medical_treatment: bool = False
    surgical_procedures: bool = False
    diagnostic_screening: bool = False
    management_supply_blood: bool = False
    transport_services: bool = False
    maternity_midwifery: bool = False
    termination_pregnancies: bool = False
    services_slimming: bool = False
    nursing_care: bool = False
    personal_care: bool = False
    accommodation_persons_detoxification: bool = False
    accommodation_persons_past_present_alcohol_dependence: bool = False
    family_planning: bool = False
    
    # Service Types (Y/N flags)
    acute_services_with_overnight_beds: bool = False
    acute_services_without_overnight_beds: bool = False
    ambulance_service: bool = False
    blood_and_transplant_service: bool = False
    care_home_nursing: bool = False
    care_home_without_nursing: bool = False
    community_based_services_substance_misuse: bool = False
    community_based_services_learning_disability: bool = False
    community_based_services_mental_health: bool = False
    community_health_care_independent_midwives: bool = False
    community_health_care_nurses_agency: bool = False
    community_health_care: bool = False
    dental_service: bool = False
    diagnostic_screening_service: bool = False
    diagnostic_screening_single_handed_sessional: bool = False
    doctors_consultation: bool = False
    doctors_treatment: bool = False
    domiciliary_care: bool = False
    extra_care_housing: bool = False
    hospice_services: bool = False
    hospice_services_at_home: bool = False
    hospital_services_mental_health_learning_disabilities: bool = False
    hospital_services_acute: bool = False
    hyperbaric_chamber: bool = False
    long_term_conditions: bool = False
    mobile_doctors: bool = False
    prison_healthcare: bool = False
    rehabilitation_services: bool = False
    remote_clinical_advice: bool = False
    residential_substance_misuse_treatment: bool = False
    shared_lives: bool = False
    specialist_college: bool = False
    supported_living: bool = False
    urgent_care: bool = False
    
    # Service User Bands (Y/N flags)
    children_0_18_years: bool = False
    dementia: bool = False
    learning_disabilities_autistic: bool = False
    mental_health_needs: bool = False
    older_people_65_plus: bool = False
    people_detained_mental_health_act: bool = False
    people_who_misuse_drugs_alcohol: bool = False
    people_with_eating_disorder: bool = False
    physical_disability: bool = False
    sensory_impairment: bool = False
    whole_population: bool = False
    younger_adults: bool = False
    
    # Legacy user band fields
    children_0_3_years: bool = False
    children_4_12_years: bool = False
    children_13_18_years: bool = False
    adults_18_65_years: bool = False


class LocationActivityFlagsCreate(LocationActivityFlagsBase):
    pass


class LocationActivityFlags(LocationActivityFlagsBase):
    id: int

    class Config:
        from_attributes = True