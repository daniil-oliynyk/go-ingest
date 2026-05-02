package transform

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/daniil-oliynyk/go-ingest/internal/model"
)

type ListingMapper struct{}

func NewListingMapper() *ListingMapper {
	return &ListingMapper{}
}

func (m *ListingMapper) MapListings(raw []map[string]any, runID string) ([]model.Listing, error) {
	log.Printf("transform: map listings started run_id=%s raw_rows=%d", runID, len(raw))
	if runID == "" {
		log.Println("transform: map listings failed: run id is empty")
		return nil, fmt.Errorf("run id is required")
	}
	if len(raw) == 0 {
		log.Println("transform: map listings failed: raw listings are empty")
		return nil, fmt.Errorf("no source listings to map")
	}

	now := time.Now().UTC()
	listings := make([]model.Listing, 0, len(raw))

	for i, row := range raw {
		id, err := stringFromKeys(row, "operator_registration_number", "registration_number", "id", "_id", "listing_id")
		if err != nil {
			log.Printf("transform: row mapping failed row=%d err=%v", i, err)
			return nil, fmt.Errorf("row %d: %w", i, err)
		}

		address, err := stringFromKeys(row, "address")
		if err != nil {
			log.Printf("transform: row mapping failed row=%d listing_id=%s stage=address err=%v", i, id, err)
			return nil, fmt.Errorf("row %d id %s: %w", i, id, err)
		}
		postalCode, err := stringFromKeys(row, "postal_code", "postal", "postcode")
		if err != nil {
			log.Printf("transform: row mapping failed row=%d listing_id=%s stage=postal_code err=%v", i, id, err)
			return nil, fmt.Errorf("row %d id %s: %w", i, id, err)
		}
		propertyType := optionalStringFromKeys(row, "property_type")
		wardNumber := optionalStringFromKeys(row, "ward_number", "ward")
		wardName := optionalStringFromKeys(row, "ward_name")

		sourceUpdatedAt, err := optionalTimeFromKeys(row, "source_updated_at", "updated_at", "last_modified", "modified")
		if err != nil {
			log.Printf("transform: row mapping failed row=%d listing_id=%s stage=source_updated_at err=%v", i, id, err)
			return nil, fmt.Errorf("row %d id %s: %w", i, id, err)
		}

		rawPayload, err := json.Marshal(row)
		if err != nil {
			log.Printf("transform: row mapping failed row=%d listing_id=%s stage=marshal_payload err=%v", i, id, err)
			return nil, fmt.Errorf("row %d id %s: marshal raw payload: %w", i, id, err)
		}

		listings = append(listings, model.Listing{
			ID:              id,
			Address:         address,
			PostalCode:      normalizePostalCode(postalCode),
			PropertyType:    propertyType,
			WardNumber:      wardNumber,
			WardName:        wardName,
			AddressKey:      normalizeAddressKey(address, postalCode),
			GeocodeQuery:    buildGeocodeQuery(address, postalCode),
			Latitude:        nil,
			Longitude:       nil,
			SourceUpdatedAt: sourceUpdatedAt,
			IngestedAt:      now,
			IngestionRunID:  runID,
			RawPayload:      rawPayload,
		})
	}
	log.Printf("transform: map listings completed run_id=%s mapped_rows=%d", runID, len(listings))

	return listings, nil
}

func normalizePostalCode(postalCode string) string {
	clean := strings.ToUpper(strings.TrimSpace(postalCode))
	clean = strings.ReplaceAll(clean, " ", "")
	return clean
}

func normalizeAddressKey(address, postalCode string) string {
	addressPart := strings.ToUpper(strings.TrimSpace(address))
	addressPart = strings.Join(strings.Fields(addressPart), " ")

	postalPart := normalizePostalCode(postalCode)

	if postalPart == "" {
		return addressPart
	}

	return addressPart + "|" + postalPart
}

func buildGeocodeQuery(address, postalCode string) string {
	addressPart := strings.TrimSpace(address)
	postalPart := normalizePostalCode(postalCode)

	if postalPart == "" {
		return fmt.Sprintf("%s, Toronto, ON, Canada", addressPart)
	}

	return fmt.Sprintf("%s, %s, Toronto, ON, Canada", addressPart, postalPart)
}

func stringFromKeys(row map[string]any, keys ...string) (string, error) {
	for _, key := range keys {
		value, ok := row[key]
		if !ok || value == nil {
			continue
		}

		switch v := value.(type) {
		case string:
			s := strings.TrimSpace(v)
			if s != "" {
				return s, nil
			}
		case float64:
			if !math.IsNaN(v) && !math.IsInf(v, 0) {
				return strconv.FormatInt(int64(v), 10), nil
			}
		case int:
			return strconv.Itoa(v), nil
		case int64:
			return strconv.FormatInt(v, 10), nil
		}
	}

	return "", fmt.Errorf("missing required string field (checked keys: %s)", strings.Join(keys, ", "))
}

func optionalStringFromKeys(row map[string]any, keys ...string) string {
	value, err := stringFromKeys(row, keys...)
	if err != nil {
		return ""
	}

	return value
}

func floatFromKeys(row map[string]any, keys ...string) (float64, error) {
	for _, key := range keys {
		value, ok := row[key]
		if !ok || value == nil {
			continue
		}

		switch v := value.(type) {
		case float64:
			if !math.IsNaN(v) && !math.IsInf(v, 0) {
				return v, nil
			}
		case float32:
			fv := float64(v)
			if !math.IsNaN(fv) && !math.IsInf(fv, 0) {
				return fv, nil
			}
		case int:
			return float64(v), nil
		case int64:
			return float64(v), nil
		case string:
			s := strings.TrimSpace(v)
			if s == "" {
				continue
			}
			parsed, err := strconv.ParseFloat(s, 64)
			if err == nil && !math.IsNaN(parsed) && !math.IsInf(parsed, 0) {
				return parsed, nil
			}
		}
	}

	return 0, fmt.Errorf("missing/invalid required numeric field (checked keys: %s)", strings.Join(keys, ", "))
}

func optionalTimeFromKeys(row map[string]any, keys ...string) (*time.Time, error) {
	for _, key := range keys {
		value, ok := row[key]
		if !ok || value == nil {
			continue
		}

		s, ok := value.(string)
		if !ok {
			continue
		}

		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}

		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			if t2, err2 := time.Parse("2006-01-02 15:04:05", s); err2 == nil {
				t = t2
			} else {
				return nil, fmt.Errorf("invalid time field %s: %w", key, err)
			}
		}

		t = t.UTC()
		return &t, nil
	}

	return nil, nil
}
