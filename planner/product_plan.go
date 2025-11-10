package planner

import (
	"github.com/kebukeYi/TrainSQL/query"
	"github.com/kebukeYi/TrainSQL/record_manager"
)

type ProductPlan struct {
	p1     Plan                   // 左表
	p2     Plan                   // 右表
	schema *record_manager.Schema // 所有新的字段;
}

func NewProductPlan(p1 Plan, p2 Plan) *ProductPlan {
	product_plan := ProductPlan{
		p1:     p1,
		p2:     p2,
		schema: record_manager.NewSchema(),
	}
	product_plan.schema.AddAll(p1.Schema())
	product_plan.schema.AddAll(p2.Schema())
	return &product_plan
}

func (p *ProductPlan) StartScan() interface{} {
	s1 := p.p1.StartScan()
	s2 := p.p2.StartScan()
	return query.NewProductScan(s1.(query.Scan), s2.(query.Scan))
}

func (p *ProductPlan) BlocksAccessed() int {
	return p.p1.BlocksAccessed() + (p.p1.RecordsOutput() * p.p2.BlocksAccessed())
}

func (p *ProductPlan) DistinctValues(fldName string) int {
	if p.p1.Schema().HasFields(fldName) {
		return p.p1.DistinctValues(fldName)
	} else {
		return p.p2.DistinctValues(fldName)
	}
}

func (p *ProductPlan) RecordsOutput() int {
	return p.p1.RecordsOutput() * p.p2.RecordsOutput()
}

func (p *ProductPlan) Schema() record_manager.SchemaInterface {
	return p.schema
}
