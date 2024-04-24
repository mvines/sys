use super::fraction::{Fraction, FractionExtra};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C, packed(1))]
pub struct BorrowRateCurve {
    pub points: [CurvePoint; 11],
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CurveSegment {
    pub slope_nom: u32,
    pub slope_denom: u32,
    pub start_point: CurvePoint,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C, packed(1))]
pub struct CurvePoint {
    pub utilization_rate_bps: u32,
    pub borrow_rate_bps: u32,
}

impl CurvePoint {
    pub fn new(utilization_rate_bps: u32, borrow_rate_bps: u32) -> Self {
        Self {
            utilization_rate_bps,
            borrow_rate_bps,
        }
    }
}

impl CurveSegment {
    pub fn from_points(start: CurvePoint, end: CurvePoint) -> Option<Self> {
        let slope_nom = end.borrow_rate_bps.checked_sub(start.borrow_rate_bps)?;
        if end.utilization_rate_bps <= start.utilization_rate_bps {
            //msg!("Utilization rate must be ever growing in the curve");
            return None;
        }
        let slope_denom = end
            .utilization_rate_bps
            .checked_sub(start.utilization_rate_bps)
            .unwrap();

        Some(CurveSegment {
            slope_nom,
            slope_denom,
            start_point: start,
        })
    }

    pub(self) fn get_borrow_rate(&self, utilization_rate: Fraction) -> Option<Fraction> {
        let start_utilization_rate = Fraction::from_bps(self.start_point.utilization_rate_bps);

        let coef = utilization_rate.checked_sub(start_utilization_rate)?;

        let nom = coef * u128::from(self.slope_nom);
        let base_rate = nom / u128::from(self.slope_denom);

        let offset = Fraction::from_bps(self.start_point.borrow_rate_bps);

        Some(base_rate + offset)
    }
}

impl BorrowRateCurve {
    pub fn get_borrow_rate(&self, utilization_rate: Fraction) -> Option<Fraction> {
        let utilization_rate = if utilization_rate > Fraction::ONE {
            /*
            msg!(
                "Warning: utilization rate is greater than 100% (scaled): {}",
                utilization_rate.to_bits()
            );
            */
            Fraction::ONE
        } else {
            utilization_rate
        };

        let utilization_rate_bps: u32 = utilization_rate.to_bps().unwrap();

        let (start_pt, end_pt) = self
            .points
            .windows(2)
            .map(|seg| {
                let [first, second]: &[CurvePoint; 2] = seg.try_into().unwrap();
                (first, second)
            })
            .find(|(first, second)| {
                utilization_rate_bps >= first.utilization_rate_bps
                    && utilization_rate_bps <= second.utilization_rate_bps
            })
            .unwrap();
        if utilization_rate_bps == start_pt.utilization_rate_bps {
            return Some(Fraction::from_bps(start_pt.borrow_rate_bps));
        } else if utilization_rate_bps == end_pt.utilization_rate_bps {
            return Some(Fraction::from_bps(end_pt.borrow_rate_bps));
        }

        let segment = CurveSegment::from_points(*start_pt, *end_pt)?;

        segment.get_borrow_rate(utilization_rate)
    }
}
